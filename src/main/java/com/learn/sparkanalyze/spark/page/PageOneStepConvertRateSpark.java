package com.learn.sparkanalyze.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.learn.sparkanalyze.constant.Constants;
import com.learn.sparkanalyze.dao.IPageSplitConvertRateDAO;
import com.learn.sparkanalyze.dao.ITaskDAO;
import com.learn.sparkanalyze.dao.factory.DAOFactory;
import com.learn.sparkanalyze.domain.PageSplitConvertRate;
import com.learn.sparkanalyze.domain.Task;
import com.learn.sparkanalyze.util.DateUtils;
import com.learn.sparkanalyze.util.NumberUtils;
import com.learn.sparkanalyze.util.ParamUtils;
import com.learn.sparkanalyze.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * Author: Xukai
 * Description: 页面单跳转化率模块spark作业
 * CreateDate: 2018/5/21 1:10
 * Modified By:
 */
public class PageOneStepConvertRateSpark {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "hadoop");
        // 1、构造Spark上下文
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);
        // conf.setMaster("local");  // 用于本地测试HiveSql

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSqlContext(sc);

        // 2、生成模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 3、查询任务，获取任务的参数
        long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);
        // 创建需要使用的DAO组件
        ITaskDAO taskDao = DAOFactory.getTaskDAO();
        Task task = taskDao.findById(taskid);
        if (task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 4、查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRdd = SparkUtils.getActionRddByDateRange(sqlContext, taskParam);
        System.out.println(actionRdd.take(20));

        // 对用户访问行为数据做一个映射，将其映射为<sessionid,访问行为>的格式
        // 用户访问页面切片的生成，是要基于每个session的访问数据，来进行生成的
        // 脱离了session，生成的页面访问切片，是没有意义的
        // 举例，比如用户A，访问了页面3和页面5
        // 用于B，访问了页面4和页面6
        // 漏了一个前提，使用者指定的页面流筛选条件，比如页面3->页面4->页面7
        // 你能不能说，是将页面3->页面4，串起来，作为一个页面切片，来进行统计呢
        // 当然不行
        // 所以说呢，页面切片的生成，肯定是要基于用户session粒度的
        JavaPairRDD<String, Row> sessionid2ActionRdd = getSessionid2ActionRdd(actionRdd);
        sessionid2ActionRdd = sessionid2ActionRdd.cache();  //persist(StorageLevel.MEMORY_ONLY());

        // 对<sessionid,访问行为> RDD，做一次groupByKey操作
        // 因为我们要拿到每个session对应的访问行为数据，才能够去生成切片
        JavaPairRDD<String, Iterable<Row>> sessionid2actionsRdd = sessionid2ActionRdd.groupByKey();

        // 最核心的一步，每个session的单跳页面切片的生成，以及页面流的匹配算法
        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(sc, sessionid2actionsRdd, taskParam);
        Map<String, Object> pageSplitPvMap = pageSplitRDD.countByKey();

        // 使用者指定的页面流是3,2,5,8,6
        // 现在拿到的这个pageSplitPvMap，3->2，2->5，5->8，8->6
        long startPagePv = getStartPagePv(taskParam, sessionid2actionsRdd);


        // 计算目标页面流的各个页面切片的转化率
        Map<String, Double> convertRateMap = computePageSplitConvertRate(taskParam, pageSplitPvMap, startPagePv);

        // 持久化页面切片转化率
        persistConvertRate(taskid, convertRateMap);

        System.out.println(pageSplitPvMap);
        System.out.println(convertRateMap);
    }

    /*
     * Author: XuKai
     * Description: 获取session到访问行为数据映射的PairRDD
     * Created: 2018/5/21 12:48
     * Params: [actionRdd]
     * Returns: org.apache.spark.api.java.JavaPairRDD<java.lang.String,org.apache.spark.sql.Row>
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRdd(JavaRDD<Row> actionRdd) {
        // 用MapPartitions操作代替Map操作
        return actionRdd.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Row>, String, Row>() {
                    @Override
                    public Iterable<Tuple2<String, Row>> call(Iterator<Row> rowIterator) throws Exception {
                        List<Tuple2<String, Row>> tuple2List = new ArrayList<Tuple2<String, Row>>();
                        while (rowIterator.hasNext()) {
                            Row row = rowIterator.next();
                            tuple2List.add(new Tuple2<String, Row>(row.getString(2), row));
                        }
                        return tuple2List;
                    }
                });
    }

    /*
     * Author: XuKai
     * Description: 页面切片生成与匹配算法
     * Created: 2018/5/21 13:13
     * Params: [sc, sessionid2ActionRdd, taskParam]
     * Returns: org.apache.spark.api.java.JavaPairRDD<java.lang.String,java.lang.Integer>
     */
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(JavaSparkContext sc, JavaPairRDD<String, Iterable<Row>> sessionid2actionsRdd, JSONObject taskParam) {
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        Broadcast<String> targetPageFlowBroadcast = sc.broadcast(targetPageFlow);
        return sessionid2actionsRdd.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
                    @Override
                    public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        // 定义返回list
                        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();
                        // 获取到当前session的访问行为的迭代器
                        Iterator<Row> iterator = tuple._2.iterator();
                        // 获取使用者指定的页面流
                        // 使用者指定的页面流，1,2,3,4,5,6,7
                        // 1->2的转化率是多少？2->3的转化率是多少？
                        String[] targetPages = targetPageFlowBroadcast.value().split(",");

                        // 以上拿到的session的访问行为，默认情况下是乱序的
                        // 比如正常情况下，希望拿到的数据按照时间顺序排序
                        // 但问题是，默认是不排序的
                        // 所以，需要对session的访问行为数据按照时间进行排序
                        // 排序按照时间升序，最近的在最后
                        ArrayList<Row> rows = new ArrayList<Row>();
                        while (iterator.hasNext()) {
                            rows.add(iterator.next());
                        }
                        Collections.sort(rows, new Comparator<Row>() {
                            @Override
                            public int compare(Row o1, Row o2) {
                                String actionTime1 = o1.getString(4);
                                String actionTime2 = o2.getString(4);

                                Date date1 = DateUtils.parseTime(actionTime1);
                                Date date2 = DateUtils.parseTime(actionTime2);

                                return (int) (date1.getTime() - date2.getTime());
                            }
                        });

                        // 页面切片的生成，以及页面流的匹配
                        Long lastPageId = null;
                        for (Row row : rows) {
                            long pageId = row.getLong(3);
                            if (lastPageId == null) {
                                lastPageId = pageId;
                                continue;
                            }

                            // 生成一个页面切片
                            // 格式为：lastPageId_thisPageId
                            // lastPageId=3，thisPageId=5，切片：3_5
                            String pageSplit = lastPageId + "_" + pageId;

                            // 判断这个切片是否在用户指定的页面流中
                            // 对这个切片判断一下，是否在用户指定的页面流中
                            for (int i = 1; i < targetPages.length; i++) {
                                // 比如说，用户指定的页面流是3,2,5,8,1
                                // 遍历的时候，从索引1开始，就是从第二个页面开始
                                // 3_2
                                String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
                                if (pageSplit.equals(targetPageSplit)) {
                                    list.add(new Tuple2<String, Integer>(pageSplit, 1));
                                    break;
                                }
                            }
                            lastPageId = pageId;
                        }

                        return list;
                    }
                }
        );
    }

    /*
     * Author: XuKai
     * Description: 获取页面流中初始页面的pv
     * Created: 2018/5/21 14:24
     * Params: [taskParam, sessionid2actionsRdd]
     * Returns: long
     */
    private static long getStartPagePv(JSONObject taskParam, JavaPairRDD<String, Iterable<Row>> sessionid2actionsRdd) {
        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);
        JavaRDD<Long> startPageRdd = sessionid2actionsRdd.flatMap(
                new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
                    @Override
                    public Iterable<Long> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        List<Long> list = new ArrayList<Long>();
                        Iterator<Row> iterator = tuple._2.iterator();
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            long pageId = row.getLong(3);
                            if (pageId == startPageId) {
                                list.add(pageId);
                            }
                        }
                        return list;
                    }
                }
        );
        return startPageRdd.count();
    }

    /*
     * Author: XuKai
     * Description: 计算页面切片转化率
     * Created: 2018/5/21 15:57
     * Params: [taskParam, pageSplitPvMap, startPagePv]
     * Returns: java.util.Map<java.lang.String,java.lang.Double>
     */
    private static Map<String, Double> computePageSplitConvertRate(JSONObject taskParam, Map<String, Object> pageSplitPvMap, long startPagePv) {
        String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");
        long lastPageSpiltPv = 0L;
        Map<String, Double> pageSplitConvertRate = new HashMap<String, Double>();

        // 3,5,2,4,6
        // 3_5 rate = 3_5 pv / 3 pv
        // 5_2 rate = 5_2 pv / 3_5 pv

        // 通过for循环，获取目标页面流中的各个页面切片（pv）
        for (int i = 1; i < targetPages.length; i++) {
            String targetPageSplit = targetPages[i - 1] + "_" + targetPages[i];
            Long targetPageSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));
            double convertRate = 0.0;
            if (i == 1) {
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPv / (double) startPagePv, 2);
            } else {
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPv / (double) lastPageSpiltPv, 2);
            }
            pageSplitConvertRate.put(targetPageSplit, convertRate);
            lastPageSpiltPv = targetPageSplitPv;
        }
        return pageSplitConvertRate;
    }


    /*
     * Author: XuKai
     * Description: 持久化转化率
     * Created: 2018/5/21 15:58
     * Params: [taskid, convertRateMap]
     * Returns: void
     */
    private static void persistConvertRate(long taskid, Map<String, Double> convertRateMap) {
        StringBuffer buffer = new StringBuffer();
        for (Map.Entry<String, Double> entry : convertRateMap.entrySet()) {
            String pageSplit = entry.getKey();
            Double convertRate = entry.getValue();
            buffer.append("|" + pageSplit + "=" + convertRate);
        }
        String convertRate = buffer.toString().replaceFirst("\\|", "");
        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate(taskid, convertRate);
        IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);
    }

}