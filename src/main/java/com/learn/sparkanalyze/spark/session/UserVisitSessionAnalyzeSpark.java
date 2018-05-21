package com.learn.sparkanalyze.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.learn.sparkanalyze.constant.Constants;
import com.learn.sparkanalyze.dao.*;
import com.learn.sparkanalyze.dao.factory.DAOFactory;
import com.learn.sparkanalyze.domain.*;
import com.learn.sparkanalyze.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.*;

/**
 * Author: Xukai
 * Description: 用户访问session分析Spark作业
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1、时间范围：起始日期~结束日期
 * 2、性别：男或女
 * 3、年龄范围
 * 4、职业：多选
 * 5、城市：多选
 * 6、搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7、点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 * 我们的spark作业如何接受用户创建的任务？
 * J2EE平台在接收用户创建任务的请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param字段中
 * 接着J2EE平台会执行我们的spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本
 * spark-submit shell脚本，在执行时，是可以接收参数的，并且会将接收的参数，传递给Spark作业的main函数
 * 参数就封装在main函数的args数组中
 * 这是spark本身提供的特性
 * CreateDate: 2018/4/28 0:48
 * Modified By:
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {

        args = new String[]{"1"};

        //构建spark上下文
        SparkConf sparkConf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_SESSION)
                /**
                 * 使用Kryo序列化优化，可以让网络传输的数据变少，在集群中耗费的内存资源减少
                 */
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                /**
                 * 比如，获取top10热门品类功能中，二次排序，自定义了一个Key
                 * 那个key需要在shuffle时进行网络传输，因此要求实现序列化
                 * 启用Kryo机制以后，就会用Kryo去序列化和反序列化CategorySortKey
                 * 所以这里要求，为了获取最佳性能，注册一下我自定义的类
                 */
                .registerKryoClasses(new Class[]{CategorySortKey.class})
                /**
                 * 调节并行度
                 */
                // .set("spark.default.parallelism", "100")
                /**
                 * 降低cache操作的内存占比，让task执行算子函数时有更多内存可以使用，默认0.6
                 */
                .set("spark.storage.memoryFraction", "0.5")
                /**
                 * 开启shuffle map端输出文件合并的机制
                 */
                .set("spark.shuffle.consolidateFiles", "true")
                /**
                 * 调节map端内存缓冲，默认32k
                 */
                .set("spark.shuffle.file.buffer", "64")
                /**
                 * 调节reduce端内存占比，默认0.2
                 */
                .set("spark.shuffle.memoryFraction", "0.3")
                /**
                 * 调节reduce端缓冲，默认48M
                 */
                .set("spark.reducer.maxSizeInFlight", "24")
                /**
                 * 下一task重试拉取文件的最大次数，默认3次
                 */
                .set("spark.shuffle.io.maxRetries", "60")
                /**
                 * 每一次重试拉取文件的时间间隔，默认5s
                 */
                .set("spark.shuffle.io.retryWait", "60");
        SparkUtils.setMaster(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = SparkUtils.getSqlContext(sc.sc());

        // 生成模拟测试数据
        SparkUtils.mockData(sc, sqlContext);

        // 创建需要使用的DAO组件
        ITaskDAO taskDao = DAOFactory.getTaskDAO();

        // 先查询出指定的任务，并获取任务的查询参数
        Long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
        Task task = taskDao.findById(taskid);
        if (task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 如果要进行session粒度的数据聚合
        // 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
        /**
         * actionRdd，就是一个公共RDD
         * 第一，要用actionRdd，获取到一个公共的sessionid为key的PairRDD
         * 第二，actionRdd，用在了session聚合环节里面
         *
         * sessionid为key的PairRDD，在后面要多次使用的
         * 1、与通过筛选的sessionid进行join，获取通过筛选的session的明细数据
         * 2、将这个RDD，直接传入aggregateBySession方法，进行session聚合统计
         *
         * 重构完以后，actionRDD，就只在最开始，使用一次，用来生成以sessionid为key的RDD
         */
        JavaRDD<Row> actionRdd = SparkUtils.getActionRddByDateRange(sqlContext, taskParam);
        JavaPairRDD<String, Row> sessionid2ActionRdd = getSessionid2ActionRdd(actionRdd);

        /**
         * 持久化，就是对RDD调用persist()方法，并传入一个持久化级别
         *
         * 如果是persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，那么就可以用cache()方法来替代
         * StorageLevel.MEMORY_ONLY_SER()，第二选择，内存+序列化
         * StorageLevel.MEMORY_AND_DISK()，第三选择，内存+磁盘
         * StorageLevel.MEMORY_AND_DISK_SER()，第四选择，内存+磁盘+序列化
         * StorageLevel.DISK_ONLY()，第五选择，纯磁盘
         *
         * 如果内存充足，要使用双副本高可靠机制
         * 选择后缀带_2的策略
         * StorageLevel.MEMORY_ONLY_2()
         */
        sessionid2ActionRdd = sessionid2ActionRdd.persist(StorageLevel.MEMORY_ONLY());
        // sessionid2ActionRdd.checkpoint();

        System.out.println(actionRdd.count());

        // 首先，可以将行为数据，按照session_id进行groupByKey分组
        // 此时数据的粒度就是session粒度，然后将session粒度的数据与用户信息数据，进行join
        // 可以获取session粒度的数据，同时数据里面还包含了session对应的user的信息
        // 获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
        JavaPairRDD<String, String> session2AggrInfoRdd = aggregateBySession(sc, sqlContext, sessionid2ActionRdd);

        System.out.println("session2AggrInfoRdd: " + session2AggrInfoRdd.count());
        for (Tuple2<String, String> tuple : session2AggrInfoRdd.take(20)) {
            System.out.println(tuple._2());
        }

        // 接着要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
        // 相当于自己编写的算子，要访问外面的任务参数对象
        // 匿名内部类（算子函数），访问外部对象，要给外部对象使用final修饰

        // 过滤统计模块,同时进行过滤和统计
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

        // 过滤session数据
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRdd = filterSessionAndAggrStat(session2AggrInfoRdd, taskParam, sessionAggrStatAccumulator);
        filteredSessionid2AggrInfoRdd = filteredSessionid2AggrInfoRdd.persist(StorageLevel.MEMORY_ONLY());

        /**
         * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明:
         * aggregateByKey算子是一个Transformation操作，必须要有action触发才能执行！！！
         * 从Accumulator中，获取数据，插入数据库的时候，一定要，一定要，在有某一个action操作以后再进行。。
         * 如果没有action的话，那么整个程序根本不会运行！
         * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take
         * 不对！！！
         * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
         * 计算出来的结果，在J2EE中，用两张柱状图显示
         */
        System.out.println("filteredSessionid2AggrInfoRdd: " + filteredSessionid2AggrInfoRdd.count());
        for (Tuple2<String, String> tuple : filteredSessionid2AggrInfoRdd.take(20)) {
            System.out.println(tuple._2());
        }
        System.out.println("sessionAggrStatAccumulator: " + sessionAggrStatAccumulator);

        // 生成公共的RDD：通过筛选条件的session的访问明细数据
        // 重构：filteredSessionid2ActionRdd，代表通过筛选的session对应的访问明细数据
        // 毕竟筛选过后的sessionDetail数据量要小得多
        JavaPairRDD<String, Row> filteredSessionid2ActionRdd = getSessionid2DetailRdd(filteredSessionid2AggrInfoRdd, sessionid2ActionRdd);
        filteredSessionid2ActionRdd = filteredSessionid2ActionRdd.persist(StorageLevel.MEMORY_ONLY());

        // 随机抽取session
        randomExtractSession(sc, taskid, filteredSessionid2AggrInfoRdd, filteredSessionid2ActionRdd);

        /**
         * 特别说明:
         * 要将上一个功能的session聚合统计数据获取到，就必须是在一个action操作触发job之后
         * 才能从Accumulator中获取数据，否则是获取不到数据的，因为没有job执行，Accumulator的值为空
         * 所以，这里将随机抽取的功能的实现代码，放在session聚合统计功能的最终计算和写库之前
         * 因为随机抽取功能中，有一个countByKey算子，是action操作，会触发job，实际上
         * count()、collect也是action操作
         */
        // 计算出各个范围的session占比，并写入MySQL
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), taskid);

        /**
         * session聚合统计（统计出访问时长和访问步长，各个区间的session数量占总session数量的比例）
         *
         * 如果不进行重构，直接来实现，思路：
         * 1、actionRDD，映射成<sessionid,Row>的格式
         * 2、按sessionid聚合，计算出每个session的访问时长和访问步长，生成一个新的RDD
         * 3、遍历新生成的RDD，将每个session的访问时长和访问步长，去更新自定义Accumulator中的对应的值
         * 4、使用自定义Accumulator中的统计值，计算各个区间的比例
         * 5、将最后计算出来的结果，写入MySQL对应的表中
         *
         * 普通实现思路的问题：
         * 1、为什么还要用actionRDD，去映射？其实之前在session聚合的时候，映射已经做过了,多此一举
         * 2、是不是一定要，为了session的聚合这个功能，单独去遍历一遍session？其实没有必要，已经有session数据
         * 	    之前过滤session的时候，就相当于在遍历session，那么这里就没有必要再过滤一遍了
         *
         * 重构实现思路：
         * 1、不要去生成任何新的RDD（处理上亿的数据）
         * 2、不要去单独遍历一遍session的数据（处理上千万的数据）
         * 3、可以在进行session聚合的时候，就直接计算出来每个session的访问时长和访问步长
         * 4、在进行过滤的时候，本来就要遍历所有的聚合session信息，此时，就可以在某个session通过筛选条件后
         * 		将其访问时长和访问步长，累加到自定义的Accumulator上面去
         *
         * 这是两种截然不同的思考方式，和实现方式，在面对上亿，上千万数据的时候，甚至可以节省时间长达半个小时，或者数个小时
         *
         * 开发Spark大型复杂项目的一些经验准则：
         * 1、尽量少生成RDD
         * 2、尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个需要做的功能
         * 3、尽量少对RDD进行shuffle算子操作，比如groupByKey、reduceByKey、sortByKey（map、mapToPair）
         * 		shuffle操作，会导致大量的磁盘读写，严重降低性能
         * 		有shuffle的算子，和没有shuffle的算子，甚至性能，会达到几十分钟，甚至数个小时的差别
         * 		有shfufle的算子，很容易导致数据倾斜，一旦数据倾斜，简直就是性能杀手（完整的解决方案）
         * 4、无论做什么功能，性能第一
         * 		在传统的J2EE或者.NET后者PHP，软件/系统/网站开发中，架构、可维护性、可扩展性的重要
         * 		程度，远远高于性能，大量的分布式的架构，设计模式，代码的划分，类的划分（高并发网站除外）
         *
         * 		在大数据项目中，比如MapReduce、Hive、Spark、Storm，性能的重要程度，远远大于代码
         * 		的规范和设计模式、代码的划分、类的划分；大数据中最重要的，就是性能
         * 		主要是因为大数据以及大数据项目的特点，决定了大数据的程序和项目的速度，都比较慢
         * 		如果不优先考虑性能，会导致一个大数据处理程序运行时间长度数个小时，甚至数十个小时
         * 		此时，对于用户体验，简直就是一场灾难
         *
         * 		所以，推荐大数据项目，在开发和代码的架构中，优先考虑性能；其次考虑功能代码的划分、解耦合
         *
         * 		我们如果采用第一种实现方案，代码划分（解耦合、可维护）优先，设计优先
         * 		如果采用第二种方案，性能优先
         *
         * 		项目开发，最重要的，除了技术本身和项目经验以外；非常重要的是积累处理各种问题的经验
         */

        // 获取Top10热门品类
        List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(taskid, filteredSessionid2ActionRdd);

        // 获取Top10热门品类中每个品类的Top10活跃Session
        getTop10Session(sc, taskid, top10CategoryList, filteredSessionid2ActionRdd);

        // 关闭spark上下文
        sc.close();
    }

    /*
     * Author: XuKai
     * Description: 获取session到访问行为数据映射的PairRDD
     * Created: 2018/5/4 13:46
     * Params: [actionRdd]
     * Returns: void
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRdd(JavaRDD<Row> actionRdd) {

        // actionRdd中的元素是Row，一个Row就是一行用户访问行为记录，比如一次点击或搜索
        // 需要将Row映射成<sessionid, Row>的格式
        // return actionRdd.mapToPair(
        //         /*
        //          * Author: XuKai
        //          * Description: 第一个参数相当于函数的输入
        //          * 第二个和第三个参数，相当于函数的输出（Tuple），分别是Tuple的第一个和第二个值
        //          * Created: 2018/4/28 11:18
        //          * Params: [actionRdd]
        //          */
        //         new PairFunction<Row, String, Row>() {
        //             @Override
        //             public Tuple2<String, Row> call(Row row) throws Exception {
        //                 return new Tuple2<String, Row>(row.getString(2), row);
        //             }
        //         });

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
     * Description: 对行为数据按session粒度进行聚合
     * Created: 2018/4/28 11:10
     * Params: [actionRdd]
     */
    private static JavaPairRDD<String, String> aggregateBySession(
            JavaSparkContext sc, SQLContext sqlContext, JavaPairRDD<String, Row> sessionid2ActionRdd) {

        // 对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRdd = sessionid2ActionRdd.groupByKey();

        // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
        JavaPairRDD<Long, String> userid2PartAggrInfoRdd = sessionid2ActionsRdd.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionid = tuple._1();
                        Iterator<Row> iterator = tuple._2.iterator();

                        // 所有搜索词和点击品类的拼接
                        StringBuffer keywordsBuffer = new StringBuffer();
                        StringBuffer clickCategoryIdsBuffer = new StringBuffer();

                        Long userid = null;

                        // session的访问时间
                        Date starttime = null;
                        Date endtime = null;

                        // session的访问步长
                        int stepLength = 0;

                        // 遍历session所有的访问行为
                        while (iterator.hasNext()) {
                            // 提取每个访问行为的搜索词字段和点击品类字段
                            Row row = iterator.next();
                            if (userid == null) {
                                userid = row.getLong(1);
                            }

                            // 实际上并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段
                            // 只有搜索行为，有searchKeyword字段
                            // 只有点击品类的行为，有clickCategoryId字段
                            // 所以，任何一行行为数据都不可能两个字段都有，所以数据是可能出现null值的
                            // 因此先判断是否为null，再将搜索词或点击品类id拼接到字符串中去
                            // 其次，之前的Buffer字符串中还必须没有这个搜索词或者点击品类id
                            if (!row.isNullAt(5)) {
                                String searchKeyword = row.getString(5);
                                if (StringUtils.isNotEmpty(searchKeyword)) {
                                    if (!keywordsBuffer.toString().contains(searchKeyword)) {
                                        keywordsBuffer.append(searchKeyword + ",");
                                    }
                                }
                            }
                            if (!row.isNullAt(6)) {
                                Long clickCategoryId = row.getLong(6);
                                if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                                    clickCategoryIdsBuffer.append(clickCategoryId + ",");
                                }
                            }

                            // 计算session开始和结束时间
                            if (!row.isNullAt(4)) {
                                Date actionTime = DateUtils.parseTime(row.getString(4));
                                if (starttime == null) {
                                    starttime = actionTime;
                                }
                                if (endtime == null) {
                                    endtime = actionTime;
                                }
                                if (actionTime.before(starttime)) {
                                    starttime = actionTime;
                                }
                                if (actionTime.after(endtime)) {
                                    endtime = actionTime;
                                }
                            }
                            // 计算session访问步长
                            stepLength++;
                        }

                        String keywords = StringUtils.trimComma(keywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

                        // 计算session访问时长（秒）
                        long visitLength = (endtime.getTime() - starttime.getTime()) / 1000;

                        // 此方法需要返回的数据格式，即使<sessionid, partAggrInfo>
                        // 但是，这一步聚合完了以后，还需要将每一行数据跟对应的用户信息进行聚合
                        // 问题是跟用户信息进行聚合的话，那么key就不应该是sessionid
                        // 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
                        // 如果这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
                        // 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举
                        // 这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
                        // 然后跟用户信息join时，将partAggrInfo关联上userInfo
                        // 再直接将返回的Tuple的key设置成sessionid
                        // 最后的数据格式为<sessionid,fullAggrInfo>

                        // 聚合数据，用什么样的格式进行拼接？
                        // 这里统一定义，使用key=value|key=value
                        String partAggrInfo = ((StringUtils.isEmpty(sessionid) ? "" : ("|" + Constants.FIELD_SESSION_ID + "=" + sessionid))
                                + (StringUtils.isEmpty(keywords) ? "" : ("|" + Constants.FIELD_SEARCH_KEYWORDS + "=" + keywords))
                                + (StringUtils.isEmpty(clickCategoryIds) ? "" : ("|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds))
                                + ("|" + Constants.FIELD_VISIT_LENGTH + "=" + visitLength)
                                + ("|" + Constants.FIELD_STEP_LENGTH + "=" + stepLength)
                                + ("|" + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(starttime)))
                                .replaceFirst("\\|", "");

                        return new Tuple2<Long, String>(userid, partAggrInfo);
                    }
                }
        );

        // 查询所有用户数据
        String sql = "select * from user_info";
        JavaRDD<Row> userInfoRdd = sqlContext.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userid2InfoRdd = userInfoRdd.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        Long userid = row.getLong(0);
                        return new Tuple2<Long, Row>(userid, row);
                    }
                }
        );

        // 将session粒度聚合数据，与用户信息进行join
        /**
         * 说明：
         * 比较适合采用reduce join转换为map join的方式
         * userid2PartAggrInfoRDD：数据量比较大，比如1千万数据
         * userid2InfoRDD：可能数据量比较小，比如用户数量才10万
         */
        // 常规做法：
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRdd = userid2PartAggrInfoRdd.join(userid2InfoRdd);
        // 对join得到的数据进行拼接，并且返回<sessionid, fullAggrInfo>格式的数据
        JavaPairRDD<String, String> sessionid2FullAggrInfoRdd = userid2FullInfoRdd.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                        String partAggrInfo = tuple._2()._1();
                        Row userInfoRow = tuple._2()._2();

                        Integer age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo
                                + ("|" + Constants.FIELD_AGE + "=" + age)
                                + (StringUtils.isEmpty(professional) ? "" : ("|" + Constants.FIELD_PROFESSIONAL + "=" + professional))
                                + (StringUtils.isEmpty(city) ? "" : ("|" + Constants.FIELD_CITY + "=" + city))
                                + (StringUtils.isEmpty(sex) ? "" : ("|" + Constants.FIELD_SEX + "=" + sex));

                        String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                        return new Tuple2<>(sessionId, fullAggrInfo);
                    }
                }
        );

        // // 以将session粒度聚合数据join用户信息为例，演示将reduce join转换为map join
        // List<Tuple2<Long, Row>> userInfos = userid2InfoRdd.collect();
        // Map<Long, Row> userInfoMap = new HashMap<Long, Row>();
        // for (Tuple2<Long, Row> userInfo : userInfos) {
        //     userInfoMap.put(userInfo._1, userInfo._2);
        // }
        // Broadcast<Map<Long, Row>> userInfoMapBroadcast = sc.broadcast(userInfoMap);
        // JavaPairRDD<String, String> userid2FullInfoRdd = userid2PartAggrInfoRdd.mapToPair(
        //         new PairFunction<Tuple2<Long, String>, String, String>() {
        //             @Override
        //             public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
        //                 Map<Long, Row> userInfoMap = userInfoMapBroadcast.value();
        //                 String partAggrInfo = tuple._2;
        //                 Row userInfoRow = userInfoMap.get(tuple._1);
        //
        //                 Integer age = userInfoRow.getInt(3);
        //                 String professional = userInfoRow.getString(4);
        //                 String city = userInfoRow.getString(5);
        //                 String sex = userInfoRow.getString(6);
        //
        //                 String fullAggrInfo = partAggrInfo
        //                         + ("|" + Constants.FIELD_AGE + "=" + age)
        //                         + (StringUtils.isEmpty(professional) ? "" : ("|" + Constants.FIELD_PROFESSIONAL + "=" + professional))
        //                         + (StringUtils.isEmpty(city) ? "" : ("|" + Constants.FIELD_CITY + "=" + city))
        //                         + (StringUtils.isEmpty(sex) ? "" : ("|" + Constants.FIELD_SEX + "=" + sex));
        //                 String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
        //
        //                 return new Tuple2<>(sessionId, fullAggrInfo);
        //             }
        //         }
        // );

        // // 以session粒度聚合数据join用户信息为例，演示sample采样倾斜key单独进行join
        // // 对所有数据抽样，false表示元素不能被重复抽样（BernoulliSampler伯努利采样器），抽样比例10%，随机数生成器的种子为9
        // JavaPairRDD<Long, String> sampledRdd = userid2PartAggrInfoRdd.sample(false, 0.1, 9);
        // // 将原RDD映射为<key, 1>形式
        // JavaPairRDD<Long, Long> mappedSampledRDD = sampledRdd.mapToPair(
        //         new PairFunction<Tuple2<Long, String>, Long, Long>() {
        //             @Override
        //             public Tuple2<Long, Long> call(Tuple2<Long, String> tuple) throws Exception {
        //                 return new Tuple2<Long, Long>(tuple._1, 1L);
        //             }
        //         }
        // );
        // // 对<key, 1>进行reduce计算，统计key的数量
        // JavaPairRDD<Long, Long> computedSampledRDD = mappedSampledRDD.reduceByKey(
        //         new Function2<Long, Long, Long>() {
        //             @Override
        //             public Long call(Long v1, Long v2) throws Exception {
        //                 return v1 + v2;
        //             }
        //         }
        // );
        // // 将<key, count>转换为<count, key>形式，用于排序
        // JavaPairRDD<Long, Long> reversedSampledRDD = computedSampledRDD.mapToPair(
        //         new PairFunction<Tuple2<Long, Long>, Long, Long>() {
        //             @Override
        //             public Tuple2<Long, Long> call(Tuple2<Long, Long> tuple) throws Exception {
        //                 return new Tuple2<Long, Long>(tuple._2, tuple._1);
        //             }
        //         }
        // );
        // // 将<count, key>按照count数量倒序排序，得到数量最多的key，就是要被单独拿出来的key
        // final Long skewedUserid = reversedSampledRDD.sortByKey(false).take(1).get(0)._2;
        // // 根据key是否为skewedUserid，对需要单独join的key拿出来，将原RDD分成两份
        // // 由于skewedUserid对应的数据量过大，被单独拿出来处理
        // JavaPairRDD<Long, String> skewedRDD = userid2PartAggrInfoRdd.filter(
        //         new Function<Tuple2<Long, String>, Boolean>() {
        //             @Override
        //             public Boolean call(Tuple2<Long, String> tuple) throws Exception {
        //                 return tuple._1.equals(skewedUserid);
        //             }
        //         }
        // );
        // // 普通key
        // JavaPairRDD<Long, String> commonRDD = userid2PartAggrInfoRdd.filter(
        //         new Function<Tuple2<Long, String>, Boolean>() {
        //             @Override
        //             public Boolean call(Tuple2<Long, String> tuple) throws Exception {
        //                 return !tuple._1.equals(skewedUserid);
        //             }
        //         }
        // );
        // // 处理userid2InfoRdd，过滤出userid为skewedUserid的数据
        // // 既然要解决数据倾斜问题，就要对skewedUserid进行处理，否则仍然是一大堆数据一起join
        // // 处理方式就是给这个特殊的key加上前缀（0-10），一个key就被分成10个key
        // JavaPairRDD<String, Row> skewedUserid2infoRDD = userid2InfoRdd.filter(
        //         new Function<Tuple2<Long, Row>, Boolean>() {
        //             @Override
        //             public Boolean call(Tuple2<Long, Row> tuple) throws Exception {
        //                 return tuple._1.equals(skewedUserid);
        //             }
        //         }
        // ).flatMapToPair(
        //         new PairFlatMapFunction<Tuple2<Long, Row>, String, Row>() {
        //             @Override
        //             public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple) throws Exception {
        //                 ArrayList<Tuple2<String, Row>> list = new ArrayList<>();
        //                 for (int prefix = 0; prefix < 10; prefix++) {
        //                     list.add(new Tuple2<String, Row>(prefix + "_" + tuple._1, tuple._2));
        //                 }
        //                 return list;
        //             }
        //         }
        // );
        // // 处理skewedRDD，给key加上随机数（0-10）前缀，这样与skewedUserid2infoRDD做join时，
        // // 由于key不同，目前最多分成10个key，就能将数据分散到不同的task，如果task设置的数量足够多，
        // // 每个task分到的数据量会大大减小，这就解决了数据倾斜问题
        // JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD1 = skewedRDD.mapToPair(
        //         new PairFunction<Tuple2<Long, String>, String, String>() {
        //             @Override
        //             public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
        //                 Random random = new Random();
        //                 int prefix = random.nextInt(10);
        //                 return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
        //             }
        //         }
        // ).join(skewedUserid2infoRDD).mapToPair(
        //         new PairFunction<Tuple2<String, Tuple2<String, Row>>, Long, Tuple2<String, Row>>() {
        //             @Override
        //             public Tuple2<Long, Tuple2<String, Row>> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
        //                 Long userid = Long.valueOf(tuple._1.split("_")[1]);
        //                 return new Tuple2<Long, Tuple2<String, Row>>(userid, tuple._2);
        //             }
        //         }
        // );
        // // 处理commonRDD，只需要与userid2InfoRdd做普通的join即可
        // JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD2 = commonRDD.join(userid2InfoRdd);
        // // 将joinedRDD1和joinedRDD2聚合，即最后结果
        // // 注意！！！
        // // （1）因为后续使用了自定义Aggregate算子，这里的union必须先使用action触发执行计算，否则Aggregate算子将无效
        // // （2）union算子计算后自动分成两个partition，这样的话而自定义Aggregate算子只会计算第一个partition，必须对结果进
        // //      行repartition，使结果只有一个分区
        // JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD = joinedRDD1.union(joinedRDD2).repartition(1);
        // JavaPairRDD<String, String> sessionid2FullAggrInfoRdd2 = joinedRDD.mapToPair(
        //         new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
        //             @Override
        //             public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
        //                 String partAggrInfo = tuple._2()._1();
        //                 Row userInfoRow = tuple._2()._2();
        //
        //                 Integer age = userInfoRow.getInt(3);
        //                 String professional = userInfoRow.getString(4);
        //                 String city = userInfoRow.getString(5);
        //                 String sex = userInfoRow.getString(6);
        //
        //                 String fullAggrInfo = partAggrInfo
        //                         + ("|" + Constants.FIELD_AGE + "=" + age)
        //                         + (StringUtils.isEmpty(professional) ? "" : ("|" + Constants.FIELD_PROFESSIONAL + "=" + professional))
        //                         + (StringUtils.isEmpty(city) ? "" : ("|" + Constants.FIELD_CITY + "=" + city))
        //                         + (StringUtils.isEmpty(sex) ? "" : ("|" + Constants.FIELD_SEX + "=" + sex));
        //
        //                 String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
        //
        //                 return new Tuple2<>(sessionId, fullAggrInfo);
        //             }
        //         }
        // );

        // // 以session粒度聚合数据join用户信息为例，演示使用随机数以及扩容表进行join
        // // 对userid2InfoRdd的key进行扩容，随机数范围为0-10
        // JavaPairRDD<String, Row> expandedRDD = userid2InfoRdd.flatMapToPair(
        //         new PairFlatMapFunction<Tuple2<Long, Row>, String, Row>() {
        //             @Override
        //             public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple) throws Exception {
        //                 List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
        //                 for(int i = 0; i < 10; i++) {
        //                     list.add(new Tuple2<String, Row>(i + "_" + tuple._1, tuple._2));
        //                 }
        //                 return list;
        //             }
        //         }
        // );
        // JavaPairRDD<String, String> sessionid2FullAggrInfoRdd3 = userid2PartAggrInfoRdd.mapToPair(
        //         new PairFunction<Tuple2<Long, String>, String, String>() {
        //             @Override
        //             public Tuple2<String, String> call(Tuple2<Long, String> tuple) throws Exception {
        //                 Random random = new Random();
        //                 int prefix = random.nextInt(10);
        //                 return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
        //             }
        //         }
        // ).join(expandedRDD).mapToPair(
        //         new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, String>() {
        //             @Override
        //             public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
        //                 String partAggrInfo = tuple._2()._1();
        //                 Row userInfoRow = tuple._2()._2();
        //
        //                 Integer age = userInfoRow.getInt(3);
        //                 String professional = userInfoRow.getString(4);
        //                 String city = userInfoRow.getString(5);
        //                 String sex = userInfoRow.getString(6);
        //
        //                 String fullAggrInfo = partAggrInfo
        //                         + ("|" + Constants.FIELD_AGE + "=" + age)
        //                         + (StringUtils.isEmpty(professional) ? "" : ("|" + Constants.FIELD_PROFESSIONAL + "=" + professional))
        //                         + (StringUtils.isEmpty(city) ? "" : ("|" + Constants.FIELD_CITY + "=" + city))
        //                         + (StringUtils.isEmpty(sex) ? "" : ("|" + Constants.FIELD_SEX + "=" + sex));
        //
        //                 String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
        //
        //                 return new Tuple2<>(sessionId, fullAggrInfo);
        //             }
        //         }
        // );

        return sessionid2FullAggrInfoRdd;
    }

    /*
     * Author: XuKai
     * Description: 过滤session数据
     * Created: 2018/5/2 11:30
     * Params: [sessionid2AggrInfoRdd]
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRdd,
            final JSONObject taskParam,
            Accumulator<String> sessionAggrStatAccumulator) {

        // 为了使用后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
        // 这里并不是多此一举，而是给后面的性能优化埋下了一个伏笔
        String startage = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endage = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String prosessionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = ((StringUtils.isEmpty(startage) ? "" : ("|" + Constants.PARAM_START_AGE + "=" + startage))
                + (StringUtils.isEmpty(endage) ? "" : ("|" + Constants.PARAM_END_AGE + "=" + endage))
                + (StringUtils.isEmpty(prosessionals) ? "" : ("|" + Constants.PARAM_PROFESSIONALS + "=" + prosessionals))
                + (StringUtils.isEmpty(cities) ? "" : ("|" + Constants.PARAM_CITIES + "=" + cities))
                + (StringUtils.isEmpty(sex) ? "" : ("|" + Constants.PARAM_SEX + "=" + sex))
                + (StringUtils.isEmpty(keywords) ? "" : ("|" + Constants.PARAM_KEYWORDS + "=" + keywords))
                + (StringUtils.isEmpty(categoryIds) ? "" : ("|" + Constants.PARAM_CATEGORY_IDS + "=" + categoryIds)))
                .replaceFirst("\\|", "");

        final String parameter = _parameter;

        // 根据筛选参数进行过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRdd = sessionid2AggrInfoRdd.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        // 首先从tuple中获取聚合数据
                        String aggrInfo = tuple._2();

                        // 依次按照筛选条件进行过滤
                        // 按照年龄范围进行过滤（startage、endage）
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        // 按照职业范围进行过滤（professionals）
                        // 互联网,IT,软件
                        // 互联网
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        // 按照城市范围进行过滤（cities）
                        // 北京,上海,广州,深圳
                        // 成都
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
                            return false;
                        }

                        // 按照性别进行过滤
                        // 男/女
                        // 男，女
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照搜索词进行过滤
                        // session可能搜索了 火锅,蛋糕,烧烤
                        // 筛选条件可能是 火锅,串串香,iphone手机
                        // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
                        // 任何一个搜索词相当，即通过
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        // 按照点击品类id进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        // 如果经过了之前的多个过滤条件之后，程序能够走到这里
                        // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
                        // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
                        // 进行相应的累加计数

                        // 走到这一步，那么就是需要计数的session
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        // 计算出session的访问时长和访问步长的范围，进行相应的累加
                        String visit_length_str = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH);
                        String step_length_str = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH);
                        long visitLength = StringUtils.isNotEmpty(visit_length_str) ? Long.valueOf(visit_length_str) : 0;
                        long stepLength = StringUtils.isNotEmpty(step_length_str) ? Long.valueOf(step_length_str) : 0;

                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);

                        return true;
                    }

                    /*
                     * Author: XuKai
                     * Description: 计算访问时长范围
                     * Created: 2018/5/3 10:24
                     * Params: [visitLength]
                     * Returns: void
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /*
                     * Author: XuKai
                     * Description: 计算访问步长范围
                     * Created: 2018/5/3 10:24
                     * Params: [stepLength]
                     * Returns: void
                     */
                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }

                }
        );
        return filteredSessionid2AggrInfoRdd;
    }

    /*
     * Author: XuKai
     * Description: 获取符合条件的session的访问明细
     * Created: 2018/5/8 1:19
     * Params: [filteredSessionid2AggrInfoRdd, sessionid2ActionRdd]
     * Returns: void
     */
    private static JavaPairRDD<String, Row> getSessionid2DetailRdd(JavaPairRDD<String, String> filteredSessionid2AggrInfoRdd, JavaPairRDD<String, Row> sessionid2ActionRdd) {
        JavaPairRDD<String, Row> sessionid2DetailRdd = filteredSessionid2AggrInfoRdd.join(sessionid2ActionRdd).mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
                        return new Tuple2<String, Row>(tuple._1, tuple._2._2);
                    }
                }
        );
        return sessionid2DetailRdd;
    }

    /*
     * Author: XuKai
     * Description: 随机抽取session
     * Created: 2018/5/3 17:09
     * Params: [sessionid2AggrInfoRdd]
     * Returns: void
     */
    private static void randomExtractSession(
            JavaSparkContext sc, final long taskid,
            JavaPairRDD<String, String> sessionid2AggrInfoRdd,
            JavaPairRDD<String, Row> filteredSessionid2ActionRdd) {

        // 第一步：计算每天每小时的session数量，获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
        JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRdd.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
                        String fullAggrInfo = tuple._2();
                        String starttime = StringUtils.getFieldFromConcatString(fullAggrInfo, "\\|", Constants.FIELD_START_TIME);
                        String dateHour = DateUtils.getDateHour(starttime);
                        return new Tuple2<String, String>(dateHour, fullAggrInfo);
                    }
                }
        );

        /*
         * 思考一下：不要着急写大量的代码，做项目的时候，一定要多思考
         * 每天每小时的session数量，然后计算出每天每小时的session抽取索引，遍历每天每小时session
         * 首先抽取出的session的聚合数据，写入session_random_extract表
         * 所以第一个RDD的value，应该是session聚合数据
         */

        // 得到每天每小时的session数量
        Map<String, Object> countMap = time2sessionidRDD.countByKey();

        // 第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引
        // 将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
        Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();
        for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            long count = Long.valueOf(String.valueOf(countEntry.getValue()));

            Map<String, Long> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<String, Long>();
                dateHourCountMap.put(date, hourCountMap);
            }
            hourCountMap.put(hour, count);
        }

        // 开始实现按时间比例随机抽取算法
        // 总共要抽取100个session，先按照天数，进行平分
        int extractNumberPerDay = 100 / dateHourCountMap.size();

        // 定义抽取session的Map格式：<date,<hour,(3,5,20,102)>>
        /*
         * session随机抽取功能
         * 用到了一个比较大的变量，随机抽取索引map
         * 之前是直接在算子里面使用了这个map，那么根据我们刚才讲的这个原理，每个task都会拷贝一份map副本
         * 比较消耗内存和网络传输性能
         * 解决方案：将map做成广播变量
         */
        /*
        * 除了广播变量技术外，这里还使用fastutil
        * 比如List<Integer>的list，对应到fastutil是IntList，
        * 使用上无差别，应为接口基本相同
        */
        // Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();
        Map<String, Map<String, IntList>> fastutilDateHourExtractMap = new HashMap<String, Map<String, IntList>>();

        Random random = new Random();
        for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

            // 计算出这一天的session总数
            long sessionCount = 0L;
            for (Long hourCout : hourCountMap.values()) {
                sessionCount += hourCout;
            }

            Map<String, IntList> fastutilHourExtractMap = fastutilDateHourExtractMap.get(date);
            if (fastutilHourExtractMap == null) {
                fastutilHourExtractMap = new HashMap<String, IntList>();
                fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
            }

            // 遍历每个小时
            for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();

                // 计算每个小时的session数量，占据当天总session数量的比例
                // 直接乘以每天要抽取的数量，就可以计算出，当前小时需要抽取的session数量
                int hourExtraceNumber = (int) (((double) count / (double) sessionCount) * extractNumberPerDay);

                // 如果hourExtraceNumber比count还大
                if (hourExtraceNumber > count) {
                    hourExtraceNumber = (int) count;
                }

                // 先获取当前小时的存放随机数的list
                IntList fastutilExtractIndexList = fastutilHourExtractMap.get(hour);
                if (fastutilExtractIndexList == null) {
                    fastutilExtractIndexList = new IntArrayList();
                    fastutilHourExtractMap.put(hour, fastutilExtractIndexList);
                }

                // 生成上面计算出来的数量的随机数
                for (int i = 0; i < hourExtraceNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    // 如果列表里已经有这个随机索引，就重新生成随机索引，避免重复
                    while (fastutilExtractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    fastutilExtractIndexList.add(extractIndex);
                }
            }
        }

        // 广播变量：调用SparkContext的broadcast()方法，传入要广播的变量即可
        final Broadcast<Map<String, Map<String, IntList>>> fastutilDateHourExtractMapBroadcast = sc.broadcast(fastutilDateHourExtractMap);

        // 第三步：遍历每天每小时的session，然后根据随机索引进行抽取
        // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
        JavaPairRDD<String, Iterable<String>> time2sessionsRdd = time2sessionidRDD.groupByKey();

        // 用flatMap算子，遍历所有的<dateHour,(session aggrInfo)>格式的数据
        // 会遍历每天每小时的session
        // 如果发现某个session恰巧在我们指定的这天这小时的随机抽取索引上
        // 那么抽取该session，直接写入MySQL的random_extract_session表
        // 将抽取出来的session id返回，形成一个新的JavaRDD<String>
        // 然后最后一步，是用抽取出来的sessionid，去join它们的访问行为明细数据，写入session表
        JavaPairRDD<String, String> extractSessionidsRdd = time2sessionsRdd.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple) throws Exception {

                        List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();

                        String dateHour = tuple._1;
                        String date = dateHour.split("_")[0];
                        String hour = dateHour.split("_")[1];

                        /**
                         * 使用广播变量：直接调用广播变量（Broadcast类型）的value() / getValue()
                         * 可以获取到之前封装的广播变量
                         */
                        Map<String, Map<String, IntList>> fastutilDateHourExtractMapFromBC = fastutilDateHourExtractMapBroadcast.value();
                        List<Integer> extractIndexList = fastutilDateHourExtractMapFromBC.get(date).get(hour);

                        Iterator<String> iterator = tuple._2.iterator();
                        int index = 0;

                        ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

                        while (iterator.hasNext()) {
                            String sessionAggrInfo = iterator.next();
                            // 如果要抽取的列表包含当前索引，就抽取
                            if (extractIndexList.contains(index)) {

                                // 将数据写入MySQL
                                String sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
                                String startTime = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME);
                                String searchKeywords = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS);
                                String clickCategoryIds = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS);
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract(taskid, sessionid, startTime, searchKeywords, clickCategoryIds);
                                sessionRandomExtractDAO.insert(sessionRandomExtract);

                                // 将sessionid加入list
                                extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
                            }
                            index++;
                        }
                        return extractSessionids;
                    }
                }
        );

        // 第四步：获取抽取的session明细数据
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRdd = extractSessionidsRdd.join(filteredSessionid2ActionRdd);
        persistSessionDetail(taskid, extractSessionDetailRdd);
    }

    /*
     * Author: XuKai
     * Description: 计算各session范围占比，并写入MySQL
     * Created: 2018/5/3 10:39
     * Params: [value, taskid]
     * Returns: void
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的占比
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble((double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble((double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble((double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble((double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble((double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble((double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble((double) visit_length_3m_10m / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble((double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble((double) visit_length_30m / (double) session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble((double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble((double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble((double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble((double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble((double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble((double) step_length_60 / (double) session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        // 调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);
    }

    /*
     * Author: XuKai
     * Description: 获取Top10热门品类
     * Created: 2018/5/7 10:13 
     * Params: [filteredSessionid2AggrInfoRdd, sessionid2ActionRdd]
     * Returns: void
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(long taskid, JavaPairRDD<String, Row> filteredSessionid2ActionRdd) {
        // 第一步：获取符合条件的session访问过的所有品类
        // 获取session访问过的所有品类id
        // 访问过：指点击过、下单过、支付过的品类
        JavaPairRDD<Long, Long> categoryidRdd = filteredSessionid2ActionRdd.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        ArrayList<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        Row row = tuple._2;
                        // 每一条数据并不是同时拥有clickCategoryid、orderCategoryid、payCategoryid，需要一一拆分
                        if (!row.isNullAt(6)) {
                            Long clickCategoryid = row.getLong(6);
                            list.add(new Tuple2<Long, Long>(clickCategoryid, clickCategoryid));
                        } else if (!row.isNullAt(8)) {
                            String orderCategoryids = row.getString(8);
                            String[] splits = orderCategoryids.split(",");
                            for (String orderCategoryid : splits) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryid), Long.valueOf(orderCategoryid)));
                            }
                        } else if (!row.isNullAt(10)) {
                            String payCategoryids = row.getString(10);
                            String[] splits = payCategoryids.split(",");
                            for (String payCategoryid : splits) {
                                list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryid), Long.valueOf(payCategoryid)));
                            }
                        }
                        return list;
                    }
                }
        );

        /**
         * 必须要进行去重
         * 如果不去重的话，会出现重复的categoryid，排序会对重复的categoryid已经countInfo进行排序
         * 最后很可能会拿到重复的数据
         */
        categoryidRdd = categoryidRdd.distinct();

        // 第二步：计算各品类的点击、下单和支付次数
        // 访问明细中，其中三种访问行为是：点击、下单和支付
        // 分别来计算各品类点击、下单和支付的次数，可以先对访问明细数据进行过滤
        // 分别过滤出点击、下单和支付行为，然后通过map、reduceByKey等算子来进行计算

        // 计算各个品类的点击次数
        JavaPairRDD<Long, Long> clickCategoryId2CountRdd = getClickCategoryId2CountRDD(filteredSessionid2ActionRdd);
        // 计算各个品类的下单次数
        JavaPairRDD<Long, Long> orderCategoryId2CountRdd = getOrderCategoryId2CountRDD(filteredSessionid2ActionRdd);
        // 计算各个品类的支付次数
        JavaPairRDD<Long, Long> payCategoryId2CountRdd = getPayCategoryId2CountRDD(filteredSessionid2ActionRdd);

        // 第三步：join各品类与它的点击、下单和支付的次数
        /**
         * categoryidRdd中包含了所有的符合条件的session访问过的品类id
         *
         * 上面分别计算出来的三份，各品类的点击、下单和支付的次数，一般都不是包含所有品类的
         * 比如，有的品类，就只是被点击过，但是从来没有人下单和支付
         * 所以，这里不能使用join操作，要使用leftOuterJoin操作，就是说，如果categoryidRdd不能
         * join到自己的某个数据，比如点击、或下单、或支付次数，那么该categoryidRdd还是要保留下来的
         * 只不过，没有join到的那个数据，就是0
         */
        // 返回的数据格式为<categoryid, "categoryid=*|clickCount=*|orderCount=*|payCount=*">
        JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(categoryidRdd, clickCategoryId2CountRdd,
                orderCategoryId2CountRdd, payCategoryId2CountRdd);

        // 第四步：自定义二次排序key

        // 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
        JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
                    @Override
                    public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
                        String countInfo = tuple._2;
                        Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                        Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                        Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
                        CategorySortKey categorySortKey = new CategorySortKey(clickCount, orderCount, payCount);
                        return new Tuple2<CategorySortKey, String>(categorySortKey, countInfo);
                    }
                }
        );
        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);

        // 第六步：用take(10)取出top10热门品类，并写入MySQL
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        List<Tuple2<CategorySortKey, String>> sortedCategoryCountList = sortedCategoryCountRDD.take(10);
        for (int i = 0; i < sortedCategoryCountList.size(); i++) {
            Tuple2<CategorySortKey, String> tuple = sortedCategoryCountList.get(i);
            String countInfo = tuple._2;
            Long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            Long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            Long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            Long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
            Top10Category top10Category = new Top10Category(taskid, categoryid, clickCount, orderCount, payCount);
            top10CategoryDAO.insert(top10Category);
        }
        return sortedCategoryCountList;
    }

    /*
     * Author: XuKai
     * Description: 获取Top10热门品类中每个品类的Top10活跃Session
     * Created: 2018/5/8 9:19
     * Params: [taskid, sessionid2DetailRdd]
     * Returns: void
     */
    private static void getTop10Session(
            JavaSparkContext sc, final Long taskid,
            List<Tuple2<CategorySortKey, String>> top10CategoryList,
            JavaPairRDD<String, Row> filteredSessionid2ActionRdd) {
        // 第一步：将top10热门品类的id，生成一份RDD
        ArrayList<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<>();
        for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
            Long categoryId = Long.valueOf(StringUtils.getFieldFromConcatString(tuple._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<Long, Long>(categoryId, categoryId));
        }

        JavaPairRDD<Long, Long> top10CategoryIdRdd = sc.parallelizePairs(top10CategoryIdList);

        // 第二步：计算top10品类被各session点击的次数
        JavaPairRDD<String, Iterable<Row>> sessionid2DetailsRdd = filteredSessionid2ActionRdd.groupByKey();
        JavaPairRDD<Long, String> categoryid2SessionCountRdd = sessionid2DetailsRdd.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();
                        HashMap<Long, Long> categoryidCountMap = new HashMap<>();
                        // 计算该session对每个品类的点击次数
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            if (!row.isNullAt(6)) {
                                long categoryid = row.getLong(6);
                                if (categoryidCountMap.containsKey(categoryid)) {
                                    Long count = categoryidCountMap.get(categoryid);
                                    count++;
                                    categoryidCountMap.put(categoryid, count);
                                } else {
                                    categoryidCountMap.put(categoryid, 0L);
                                }
                            }
                        }

                        // 返回结果：<categoryid, "sessionid,count">
                        ArrayList<Tuple2<Long, String>> tupleList = new ArrayList<>();
                        for (Map.Entry<Long, Long> entry : categoryidCountMap.entrySet()) {
                            Long categoryid = entry.getKey();
                            Long count = entry.getValue();
                            tupleList.add(new Tuple2<Long, String>(categoryid, sessionid + "," + count));
                        }
                        return tupleList;
                    }
                }
        );

        // 通过join操作：<top10CategoryId, top10CategoryId> join <categoryId, "sessionid,count">
        // 获得top10热门品类被各session点击的统计次数，此时获得的结果还未被分组
        JavaPairRDD<Long, String> top10CategorySessionCountRdd = top10CategoryIdRdd.join(categoryid2SessionCountRdd).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple) throws Exception {
                        return new Tuple2<Long, String>(tuple._1, tuple._2._2);
                    }
                }
        );

        // 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRdd = top10CategorySessionCountRdd.groupByKey();
        JavaPairRDD<String, String> top10SessionRdd = top10CategorySessionCountsRdd.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple) throws Exception {
                        Long categoryid = tuple._1;
                        Iterator<String> iterator = tuple._2.iterator();
                        // 原教程模拟实现了类似维护一个最大堆的算法，始终让较大的排在前面
                        // 但问题是，如果每次都是在第一位插入数据，后面九个数据都要向后移动一位，增加了复杂度
                        // String[] top10Sessions = new String[10];
                        // while(iterator.hasNext()) {
                        //     String sessionCount = iterator.next();
                        //     long count = Long.valueOf(sessionCount.split(",")[1]);
                        //     // 遍历排序数组
                        //     for(int i = 0; i < top10Sessions.length; i++) {
                        //         // 如果当前i位，没有数据，那么直接将i位数据赋值为当前sessionCount
                        //         if(top10Sessions[i] == null) {
                        //             top10Sessions[i] = sessionCount;
                        //             break;
                        //         } else {
                        //             long _count = Long.valueOf(top10Sessions[i].split(",")[1]);
                        //             // 如果sessionCount比i位的sessionCount要大
                        //             if(count > _count) {
                        //                 // 从排序数组最后一位开始，到i位，所有数据往后挪一位
                        //                 for(int j = 9; j > i; j--) {
                        //                     top10Sessions[j] = top10Sessions[j - 1];
                        //                 }
                        //                 // 将i位赋值为sessionCount
                        //                 top10Sessions[i] = sessionCount;
                        //                 break;
                        //             }
                        //             // 比较小，继续外层for循环
                        //         }
                        //     }
                        // }

                        // 利用TreeMap的排序特性实现取TopN
                        // 定义TreeMap，重构排序规则，按照Key降序排列
                        TreeMap<Long, String> top10Sessions = new TreeMap<Long, String>(new Comparator<Long>() {
                            public int compare(Long a, Long b) {
                                return (int) (b - a);
                            }
                        });
                        while (iterator.hasNext()) {
                            String sessionCount = iterator.next();
                            long count = Long.valueOf(sessionCount.split(",")[1]);
                            top10Sessions.put(count, sessionCount);
                            if (top10Sessions.size() > 10) {
                                top10Sessions.remove(top10Sessions.lastKey());
                            }
                        }

                        // 将数据写入MySQL表
                        ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                        for (Map.Entry<Long, String> entry : top10Sessions.entrySet()) {
                            String sessionid = entry.getValue().split(",")[0];
                            long count = Long.valueOf(entry.getValue().split(",")[1]);
                            // 将top10 session插入MySQL表
                            Top10Session top10Session = new Top10Session(taskid, categoryid, sessionid, count);
                            top10SessionDAO.insert(top10Session);
                            // 放入list
                            list.add(new Tuple2<String, String>(sessionid, sessionid));
                        }
                        return list;
                    }
                }
        );

        // System.out.println(top10SessionRdd.collect());  //触发action

        // 第四步：获取top10活跃session的明细数据，并写入MySQL
        /**
         * 必须要进行去重
         * 如果不去重，会出现重复的sessionid,最后很可能会拿到重复的数据
         */
        top10SessionRdd = top10SessionRdd.distinct();
        JavaPairRDD<String, Tuple2<String, Row>> top10SessionDetailRDD = top10SessionRdd.join(filteredSessionid2ActionRdd);
        persistSessionDetail(taskid, top10SessionDetailRDD);
    }


    /*
     * Author: XuKai
     * Description: 计算各个品类的点击次数
     * Created: 2018/5/7 11:40
     * Params: [sessionid2DetailRdd]
     * Returns: org.apache.spark.api.java.JavaPairRDD<java.lang.Long,java.lang.Long>
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2DetailRdd) {

        /**
         * 说明：
         * 这里对完整的数据进行filter过滤，过滤出来点击行为的数据
         * 点击行为的数据其实只占总数据的一小部分
         * 所以过滤以后的RDD，每个partition的数据量，很有可能会很不均匀，而且数据量肯定会变少很多
         * 针对这种情况，比较合适用一下coalesce算子，在filter后减少partition的数量
         */

        JavaPairRDD<String, Row> clickActionRdd = sessionid2DetailRdd.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return (row.isNullAt(6) ? false : true);
                    }
                }
        );
        // .coalesce(100);
        /**
         * 对coalesce操作说明：
         * 这里用的模式都是local模式，主要是用来测试
         * local模式自己本身就是进程内模拟的集群来执行，本身性能就很高，因此不用设置分区和并行度数量
         * 而且对并行度、partition数量都有一定的内部的优化
         */

        JavaPairRDD<Long, Long> clickCategoryIdRdd = clickActionRdd.mapToPair(
                new PairFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        Long clickCategoryId = row.getLong(6);
                        return new Tuple2<Long, Long>(clickCategoryId, 1L);
                    }
                }
        );

        /**
         * 计算各个品类的点击次数
         * 如果某个品类点击了1000万次，其他品类都是10万次，那么也会数据倾斜
         * 这里用reduceByKey演示提高shuffle操作reduce并行度
         */
        JavaPairRDD<Long, Long> clickCategoryId2CountRdd = clickCategoryIdRdd.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        // 用reduceByKey演示提高shuffle操作reduce并行度
        // JavaPairRDD<Long, Long> clickCategoryId2CountRdd = clickCategoryIdRdd.reduceByKey(
        //         new Function2<Long, Long, Long>() {
        //             @Override
        //             public Long call(Long v1, Long v2) throws Exception {
        //                 return v1 + v2;
        //             }
        //         }
        //         , 1000
        // );

        // // 用reduceByKey演示随机key实现双重聚合
        // // 第一步，给每个key打上一个随机数
        // JavaPairRDD<String, Long> mappedClickCategoryIdRDD = clickCategoryIdRdd.mapToPair(
        //         new PairFunction<Tuple2<Long, Long>, String, Long>() {
        //             @Override
        //             public Tuple2<String, Long> call(Tuple2<Long, Long> tuple) throws Exception {
        //                 Random random = new Random();
        //                 int prefix = random.nextInt(10);
        //                 return new Tuple2<String, Long>(prefix + "_" + tuple._1, tuple._2);
        //             }
        //         }
        // );
        // // 第二步，执行第一轮局部聚合
        // JavaPairRDD<String, Long> firstAggrRDD = mappedClickCategoryIdRDD.reduceByKey(
        //         new Function2<Long, Long, Long>() {
        //             @Override
        //             public Long call(Long v1, Long v2) throws Exception {
        //                 return v1 + v2;
        //             }
        //         }
        // );
        // // 第三步，去除每个key的前缀
        // JavaPairRDD<Long, Long> restoredRDD = firstAggrRDD.mapToPair(
        //         new PairFunction<Tuple2<String, Long>, Long, Long>() {
        //             @Override
        //             public Tuple2<Long, Long> call(Tuple2<String, Long> tuple) throws Exception {
        //                 long categoryId = Long.valueOf(tuple._1.split("_")[1]);
        //                 return new Tuple2<Long, Long>(categoryId, tuple._2);
        //             }
        //         }
        // );
        // // 第四步，最第二轮全局的聚合，得到最终结果
        // JavaPairRDD<Long, Long> clickCategoryId2CountRDD = restoredRDD.reduceByKey(
        //         new Function2<Long, Long, Long>() {
        //             @Override
        //             public Long call(Long v1, Long v2) throws Exception {
        //                 return v1 + v2;
        //             }
        //         }
        // );

        return clickCategoryId2CountRdd;
    }

    /*
     * Author: XuKai
     * Description: 计算各个品类的下单次数
     * Created: 2018/5/7 11:41
     * Params: [sessionid2DetailRdd]
     * Returns: org.apache.spark.api.java.JavaPairRDD<java.lang.Long,java.lang.Long>
     */
    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2DetailRdd) {
        JavaPairRDD<String, Row> orderActionRdd = sessionid2DetailRdd.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return (row.isNullAt(8) ? false : true);
                    }
                }
        );
        JavaPairRDD<Long, Long> orderCategoryIdRdd = orderActionRdd.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        Row row = tuple._2;
                        String orderCategoryids = row.getString(8);
                        String[] splits = orderCategoryids.split(",");
                        for (String orderCategoryid : splits) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryid), 1L));
                        }
                        return list;
                    }
                }
        );

        JavaPairRDD<Long, Long> orderCategoryId2CountRdd = orderCategoryIdRdd.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        return orderCategoryId2CountRdd;
    }

    /*
     * Author: XuKai
     * Description: 计算各个品类的支付次数
     * Created: 2018/5/7 11:42
     * Params: [sessionid2DetailRdd]
     * Returns: org.apache.spark.api.java.JavaPairRDD<java.lang.Long,java.lang.Long>
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2DetailRdd) {
        JavaPairRDD<String, Row> payActionRdd = sessionid2DetailRdd.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple) throws Exception {
                        Row row = tuple._2;
                        return (row.isNullAt(10) ? false : true);
                    }
                }
        );
        JavaPairRDD<Long, Long> payCategoryIdRdd = payActionRdd.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        Row row = tuple._2;
                        String orderCategoryids = row.getString(10);
                        String[] splits = orderCategoryids.split(",");
                        for (String orderCategoryid : splits) {
                            list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryid), 1L));
                        }
                        return list;
                    }
                }
        );

        JavaPairRDD<Long, Long> payCategoryId2CountRdd = payCategoryIdRdd.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        return payCategoryId2CountRdd;
    }

    /*
     * Author: XuKai
     * Description: 连接品类Rdd和数据Rdd
     * Created: 2018/5/7 13:53
     * Params: [categoryidRdd, clickCategoryId2CountRdd, orderCategoryId2CountRdd, payCategoryId2CountRdd]
     * Returns: void
     */
    private static JavaPairRDD<Long, String> joinCategoryAndData(JavaPairRDD<Long, Long> categoryidRdd, JavaPairRDD<Long, Long> clickCategoryId2CountRdd, JavaPairRDD<Long, Long> orderCategoryId2CountRdd, JavaPairRDD<Long, Long> payCategoryId2CountRdd) {
        // 如果用leftOuterJoin，就可能出现，右边那个RDD中join过来没有值
        // 所以Tuple中的第二个值用Optional<Long>类型，就代表，可能有值，可能没有值
        // <categoryid, categoryid>先与<categoryid, clickCount>做leftJoin，得到<categoryid, "categoryid=*|clickCount=*">
        JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tempJoinRdd = categoryidRdd.leftOuterJoin(clickCategoryId2CountRdd);
        JavaPairRDD<Long, String> tempMapRdd = tempJoinRdd.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple) throws Exception {
                        Long categoryid = tuple._1;
                        Optional<Long> clickCountOptional = tuple._2._2;
                        Long clickCount = 0L;
                        if (clickCountOptional.isPresent()) {
                            clickCount = clickCountOptional.get();
                        }
                        String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|"
                                + Constants.FIELD_CLICK_COUNT + "=" + clickCount;
                        return new Tuple2<Long, String>(categoryid, value);
                    }
                }
        );
        // <categoryid, "categoryid=*|clickCount=*">再与<categoryid, orderCount>做leftJoin，得到<categoryid, "categoryid=*|clickCount=*|orderCount=*">
        tempMapRdd = tempMapRdd.leftOuterJoin(orderCategoryId2CountRdd).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                        Long categoryid = tuple._1;
                        Optional<Long> orderCountOptional = tuple._2._2;
                        Long orderCount = 0L;
                        if (orderCountOptional.isPresent()) {
                            orderCount = orderCountOptional.get();
                        }
                        String value = tuple._2._1 + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
                        return new Tuple2<Long, String>(categoryid, value);
                    }
                }
        );
        // <categoryid, "categoryid=*|clickCount=*|orderCount=*">再与<categoryid, payCount>做leftJoin
        // 得到<categoryid, "categoryid=*|clickCount=*|orderCount=*|payCount=*">
        tempMapRdd = tempMapRdd.leftOuterJoin(payCategoryId2CountRdd).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple) throws Exception {
                        Long categoryid = tuple._1;
                        Optional<Long> orderCountOptional = tuple._2._2;
                        Long payCount = 0L;
                        if (orderCountOptional.isPresent()) {
                            payCount = orderCountOptional.get();
                        }
                        String value = tuple._2._1 + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;
                        return new Tuple2<Long, String>(categoryid, value);
                    }
                }
        );
        return tempMapRdd;
    }

    /*
     * Author: XuKai
     * Description: 将session详情插入数据库
     * Created: 2018/5/8 13:52
     * Params: [taskid, sessionDetailRDD]
     * Returns: void
     */
    private static void persistSessionDetail(long taskid, JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD) {

        /**
         * 使用foreachPartition代替foreach
         */
        // sessionDetailRDD.foreach(
        //         new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
        //             @Override
        //             public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
        //                 String sessionid = tuple._1;
        //                 Row row = tuple._2._2;
        //                 ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
        //                 // 判断数据库中是否已经添加了该sessionid的详情数据，如果已添加就跳过
        //                 SessionDetail sessionDetail = new SessionDetail();
        //                 sessionDetail.setSessionid(sessionid);
        //                 sessionDetail.setTaskid(taskid);
        //                 if (!row.isNullAt(1)) {
        //                     Long userid = row.getLong(1);
        //                     sessionDetail.setUserid(userid);
        //                 }
        //                 if (!row.isNullAt(3)) {
        //                     Long pageid = row.getLong(3);
        //                     sessionDetail.setPageid(pageid);
        //                 }
        //                 if (!row.isNullAt(4)) {
        //                     String actionTime = row.getString(4);
        //                     sessionDetail.setActionTime(actionTime);
        //                 }
        //                 if (!row.isNullAt(5)) {
        //                     String searchKeyword = row.getString(5);
        //                     sessionDetail.setSearchKeyword(searchKeyword);
        //                 }
        //                 if (!row.isNullAt(6)) {
        //                     Long clickCategoryId = row.getLong(6);
        //                     sessionDetail.setClickCategoryId(clickCategoryId);
        //                 }
        //                 if (!row.isNullAt(7)) {
        //                     Long clickProductId = row.getLong(7);
        //                     sessionDetail.setClickProductId(clickProductId);
        //                 }
        //                 if (!row.isNullAt(8)) {
        //                     String orderCategoryIds = row.getString(8);
        //                     sessionDetail.setOrderCategoryIds(orderCategoryIds);
        //                 }
        //                 if (!row.isNullAt(9)) {
        //                     String orderProductIds = row.getString(9);
        //                     sessionDetail.setOrderProductIds(orderProductIds);
        //                 }
        //                 if (!row.isNullAt(10)) {
        //                     String payCategoryIds = row.getString(10);
        //                     sessionDetail.setPayCategoryIds(payCategoryIds);
        //                 }
        //                 if (!row.isNullAt(11)) {
        //                     String payProductIds = row.getString(11);
        //                     sessionDetail.setPayProductIds(payProductIds);
        //                 }
        //                 if (!sessionDetailDAO.isExist(sessionDetail)) {
        //                     sessionDetailDAO.insert(sessionDetail);
        //                 }
        //
        //             }
        //         }
        // );

        sessionDetailRDD.foreachPartition(
                new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> tuple2Iterator) throws Exception {
                        while (tuple2Iterator.hasNext()) {
                            Tuple2<String, Tuple2<String, Row>> tuple = tuple2Iterator.next();
                            String sessionid = tuple._1;
                            Row row = tuple._2._2;
                            ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                            // 判断数据库中是否已经添加了该sessionid的详情数据，如果已添加就跳过
                            SessionDetail sessionDetail = new SessionDetail();
                            sessionDetail.setSessionid(sessionid);
                            sessionDetail.setTaskid(taskid);
                            if (!row.isNullAt(1)) {
                                Long userid = row.getLong(1);
                                sessionDetail.setUserid(userid);
                            }
                            if (!row.isNullAt(3)) {
                                Long pageid = row.getLong(3);
                                sessionDetail.setPageid(pageid);
                            }
                            if (!row.isNullAt(4)) {
                                String actionTime = row.getString(4);
                                sessionDetail.setActionTime(actionTime);
                            }
                            if (!row.isNullAt(5)) {
                                String searchKeyword = row.getString(5);
                                sessionDetail.setSearchKeyword(searchKeyword);
                            }
                            if (!row.isNullAt(6)) {
                                Long clickCategoryId = row.getLong(6);
                                sessionDetail.setClickCategoryId(clickCategoryId);
                            }
                            if (!row.isNullAt(7)) {
                                Long clickProductId = row.getLong(7);
                                sessionDetail.setClickProductId(clickProductId);
                            }
                            if (!row.isNullAt(8)) {
                                String orderCategoryIds = row.getString(8);
                                sessionDetail.setOrderCategoryIds(orderCategoryIds);
                            }
                            if (!row.isNullAt(9)) {
                                String orderProductIds = row.getString(9);
                                sessionDetail.setOrderProductIds(orderProductIds);
                            }
                            if (!row.isNullAt(10)) {
                                String payCategoryIds = row.getString(10);
                                sessionDetail.setPayCategoryIds(payCategoryIds);
                            }
                            if (!row.isNullAt(11)) {
                                String payProductIds = row.getString(11);
                                sessionDetail.setPayProductIds(payProductIds);
                            }
                            if (!sessionDetailDAO.isExist(sessionDetail)) {
                                sessionDetailDAO.insert(sessionDetail);
                            }
                        }
                    }
                }
        );
    }
}
