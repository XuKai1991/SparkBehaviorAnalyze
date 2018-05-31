package com.learn.analyze.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.learn.analyze.conf.ConfigurationManager;
import com.learn.analyze.constant.Constants;
import com.learn.analyze.dao.IAreaTop3ProductDAO;
import com.learn.analyze.dao.ITaskDAO;
import com.learn.analyze.dao.factory.DAOFactory;
import com.learn.analyze.domain.AreaTop3Product;
import com.learn.analyze.domain.Task;
import com.learn.analyze.util.ParamUtils;
import com.learn.analyze.util.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * Author: Xukai
 * Description: 各区域top3热门商品统计Spark作业
 * CreateDate: 2018/5/23 1:04
 * Modified By:
 */
public class AreaTop3ProductSpark {

    public static void main(String[] args) {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("AreaTop3ProductSpark");
        SparkUtils.setMaster(conf);
        // conf.setMaster("local");  // 用于本地测试HiveSql

        // 构建Spark上下文
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = SparkUtils.getSqlContext(sc);

        // 提高shuffle并行度，默认200
        // sqlContext.setConf("spark.sql.shuffle.partitions", "1000");

        // 内置的map join，如果有一个小表小于默认阈值，就会将该表进行broadcast，然后执行map join
        // 阈值默认为默认10485760（10M）
        // sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", "20971520");

        // 注册自定义函数
        sqlContext.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
        sqlContext.udf().register("get_json_object", new GetJsonObjectUDF(), DataTypes.StringType);
        sqlContext.udf().register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType);
        sqlContext.udf().register("remove_random_prefix", new RemoveRandomPrefixUDF(), DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());

        // 以下两个自定义UDF用于随机key的group
        sqlContext.udf().register("get_area", new GetAreaFromPIA(), DataTypes.StringType);
        sqlContext.udf().register("get_product_id", new GetProductIdFromPIA(), DataTypes.LongType);

        // 以下两个自定义UDF用于随机key与扩容表的join连接
        sqlContext.udf().register("long_to_string", new LongToString(), DataTypes.StringType);
        sqlContext.udf().register("string_to_long", new StringToLong(), DataTypes.LongType);

        // 生成模拟数据
        SparkUtils.mockData(sc, sqlContext);

        // 查询任务，获取任务的参数
        long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);
        ITaskDAO taskDao = DAOFactory.getTaskDAO();
        Task task = taskDao.findById(taskid);
        if (task == null) {
            System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
            return;
        }
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        // 查询用户指定日期范围内的点击行为数据（city_id，在哪个城市发生的点击行为）
        // 技术点1：Hive数据源的使用
        JavaRDD<Row> cityActionRdd = SparkUtils.getcityActionRddByDate(sqlContext, taskParam);
        JavaPairRDD<Long, Row> cityid2clickActionRdd = getCityid2ClickActionRdd(cityActionRdd);
        System.out.println("cityid2clickActionRdd: " + cityid2clickActionRdd.count());

        // 从MySQL中查询城市信息
        // 技术点2：异构数据源之MySQL的使用
        JavaPairRDD<Long, Row> cityid2CityInfoRdd = getCityid2CityInfoRdd(sqlContext);
        System.out.println("cityid2CityInfoRdd: " + cityid2CityInfoRdd.count());

        // 生成点击商品基础信息临时表
        // 技术点3：将RDD转换为DataFrame，并注册临时表
        generateTempClickProductBasicTable(sqlContext, cityid2clickActionRdd, cityid2CityInfoRdd);

        // 生成各区域各商品点击次数的临时表
        generateTempAreaPrdocutClickCountTable(sqlContext);

        // 生成包含完整商品信息的各区域各商品点击次数的临时表
        generateTempAreaFullProductClickCountTable(sqlContext);

        // 使用开窗函数获取各个区域内点击次数排名前3的热门商品
        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(sqlContext);

        // 这边的写入mysql和之前不太一样
        // 因为实际上，就这个业务需求而言，计算出来的最终数据量是比较小的
        // 总共就不到10个区域，每个区域还是top3热门商品，总共最后数据量也就是几十个
        // 所以可以直接将数据collect()到本地
        // 用批量插入的方式，一次性插入mysql即可
        List<Row> rows = areaTop3ProductRDD.collect();
        for (Row row : rows) {
            System.out.println(row);
        }
        persistAreaTop3Product(taskid, rows);

        // 关闭Spark上下文
        sc.close();
    }

    /*
     * Author: XuKai
     * Description: 获取cityid到访问行为数据映射的PairRDD
     * Created: 2018/5/23 9:48
     * Params: [cityActionRdd]
     * Returns: org.apache.spark.api.java.JavaPairRDD<java.lang.Long,org.apache.spark.sql.Row>
     */
    private static JavaPairRDD<Long, Row> getCityid2ClickActionRdd(JavaRDD<Row> cityActionRdd) {
        JavaPairRDD<Long, Row> cityid2clickActionRDD = cityActionRdd.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Row>, Long, Row>() {
                    private static final long serialVersionUID = 7281032961460664243L;

                    @Override
                    public Iterable<Tuple2<Long, Row>> call(Iterator<Row> iterator) throws Exception {
                        ArrayList<Tuple2<Long, Row>> list = new ArrayList<>();
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            list.add(new Tuple2<>(row.getLong(0), row));
                        }
                        return list;
                    }
                }
        );
        return cityid2clickActionRDD;
    }

    /*
     * Author: XuKai
     * Description: 使用SQLContext从MySql中查询城市信息
     * Created: 2018/5/23 10:10
     * Params: [sqlContext]
     * Returns: org.apache.spark.api.java.JavaRDD<org.apache.spark.sql.Row>
     */
    private static JavaPairRDD<Long, Row> getCityid2CityInfoRdd(SQLContext sqlContext) {
        // 构建MySQL连接配置信息（直接从配置文件中获取）
        String url = null;
        String user = null;
        String password = null;
        String dbtable = "city_info";
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        // 通过SQLContext去从MySQL中查询数据
        // HashMap<String, String> options = new HashMap<>();
        // options.put("url", url);
        // options.put("user", user);
        // options.put("password", password);
        // options.put("dbtable", dbtable);
        // DataFrame cityInfo = sqlContext.read().format("jdbc").options(options).load();
        // 另一种实现方式
        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", password);
        prop.put("driver", "com.mysql.jdbc.Driver");
        prop.put("fetchsize", "3");
        DataFrame cityInfo = sqlContext.read().jdbc(url, dbtable, prop);

        return cityInfo.javaRDD().mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Row>, Long, Row>() {
                    private static final long serialVersionUID = 3392626253650889664L;

                    @Override
                    public Iterable<Tuple2<Long, Row>> call(Iterator<Row> iterator) throws Exception {
                        ArrayList<Tuple2<Long, Row>> list = new ArrayList<>();
                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            list.add(new Tuple2<>(Long.valueOf(String.valueOf(row.get(0))), row));
                        }
                        return list;
                    }
                }
        );
    }

    /*
     * Author: XuKai
     * Description: 生成点击商品基础信息临时表
     * Created: 2018/5/23 10:43
     * Params: [sqlContext, cityid2clickActionRdd, cityid2CityInfoRdd]
     * Returns: void
     */
    private static void generateTempClickProductBasicTable(
            SQLContext sqlContext, JavaPairRDD<Long, Row> cityid2clickActionRdd, JavaPairRDD<Long, Row> cityid2CityInfoRdd) {

        // 执行join操作，进行点击行为数据和城市数据的关联
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = cityid2clickActionRdd.join(cityid2CityInfoRdd);

        // 将上面的JavaPairRDD，转换成一个JavaRDD<Row>（才能将RDD转换为DataFrame）
        JavaRDD<Row> mappedRDD = joinedRDD.mapPartitions(
                new FlatMapFunction<Iterator<Tuple2<Long, Tuple2<Row, Row>>>, Row>() {
                    private static final long serialVersionUID = -7146945344485446279L;

                    @Override
                    public Iterable<Row> call(Iterator<Tuple2<Long, Tuple2<Row, Row>>> iterator) throws Exception {
                        ArrayList<Row> list = new ArrayList<>();
                        while (iterator.hasNext()) {
                            Tuple2<Long, Tuple2<Row, Row>> tuple = iterator.next();
                            Long cityId = tuple._1;
                            Row clickAction = tuple._2._1;
                            Row cityInfo = tuple._2._2;

                            long productId = clickAction.getLong(1);
                            String cityName = cityInfo.getString(1);
                            String area = cityInfo.getString(2);

                            Row row = RowFactory.create(cityId, cityName, area, productId);
                            list.add(row);
                        }
                        return list;
                    }
                }
        );

        // 基于JavaRDD<Row>的格式，就可以将其转换为DataFrame
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));

        // 1 北京
        // 2 上海
        // 1 北京
        // group by area,product_id
        // 1:北京,2:上海

        // 两个函数
        // UDF：concat2()，将两个字段拼接起来，用指定的分隔符
        // UDAF：group_concat_distinct()，将一个分组中的多个字段值，用逗号拼接起来，同时进行去重

        StructType schema = DataTypes.createStructType(structFields);
        DataFrame df = sqlContext.createDataFrame(mappedRDD, schema);

        // 将DataFrame中的数据，注册成临时表（tmp_click_product_basic）
        df.registerTempTable("tmp_click_product_basic");
    }

    /*
     * Author: XuKai
     * Description: 生成各区域各商品点击次数临时表
     * Created: 2018/5/23 23:34
     * Params: [sqlContext]
     * Returns: void
     */
    private static void generateTempAreaPrdocutClickCountTable(SQLContext sqlContext) {
        // 按照area和product_id两个字段进行分组
        // 计算出各区域各商品的点击次数
        // 可以获取到每个area下的每个product_id的城市信息拼接起来的串

        String sql = "select " +
                "area," +
                "product_id," +
                "count(*) click_count," +
                "group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos " +
                "from tmp_click_product_basic " +
                "group by area,product_id";

        // 改写SQL，模仿Spark Core调优的使用随机key实现双重聚合
        // 对字段添加和去除随机key要使用自定义UDF函数
        // String sql = "select " +
        //         "get_area(product_id_area) area," +
        //         "get_product_id(product_id_area) product_id," +
        //         "count(part_click_count) click_count," +
        //         "group_concat_distinct(city_infos) city_infos " +
        //         "from (" +
        //             "select " +
        //             "remove_random_prefix(random_product_id_area) product_id_area," +
        //             "count(*) part_click_count," +
        //             "group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos " +
        //             "from (" +
        //                 "select random_prefix(concat_long_string(product_id, area, ':'),10) random_product_id_area," +
        //                 "city_id,city_name " +
        //                 "from tmp_click_product_basic" +
        //             ") t1 " +
        //             "group by random_product_id_area" +
        //         ") t2 " +
        //         "group by product_id_area";

        // 使用Spark SQL执行这条SQL语句
        DataFrame df = sqlContext.sql(sql);

        System.out.println("tmp_area_product_click_count: " + df.count());
        System.out.println(df.javaRDD().collect());

        // 再次将查询出来的数据注册为一个临时表
        // 各区域各商品的点击次数（以及额外的城市列表）
        df.registerTempTable("tmp_area_product_click_count");
    }

    /*
     * Author: XuKai
     * Description: 生成区域商品点击次数临时表（包含了商品的完整信息）
     * Created: 2018/5/24 0:33
     * Params: [sqlContext]
     * Returns: void
     */
    private static void generateTempAreaFullProductClickCountTable(SQLContext sqlContext) {
        // 将之前得到的各区域各商品点击次数表，product_id
        // 去关联商品信息表，product_id，product_name和product_status
        // product_status要特殊处理，0，1，分别代表了自营和第三方的商品，放在了一个json串里面
        // get_json_object()函数，可以从json串中获取指定的字段的值
        // if()函数，判断，如果product_status是0，那么就是自营商品；如果是1，那么就是第三方商品
        // area, product_id, click_count, city_infos, product_name, product_status

        // 为什么要费时费力，计算出来商品经营类型
        // 你拿到到了某个区域top3热门的商品，那么其实这个商品是自营的，还是第三方的
        // 其实是很重要的一件事

        // 技术点：内置if函数的使用
        String sql = "select tapcc.area," +
                "tapcc.product_id," +
                "tapcc.click_count," +
                "tapcc.city_infos," +
                "pi.product_name," +
                "if(get_json_object(pi.extend_info,'product_status')='0','Self','Third Party') product_status " +
                "from " +
                "tmp_area_product_click_count tapcc " +
                "join " +
                "sparkanalyze.product_info pi " +
                "on " +
                "tapcc.product_id = pi.product_id";

        // // 演示随机key与扩容表：Spark SQL+Spark Core
        // String tempSql = "select * from sparkanalyze.product_info";
        // JavaRDD<Row> productInfoRDD = sqlContext.sql(tempSql).javaRDD();
        // JavaRDD<Row> tempProductInfoRDD = productInfoRDD.flatMap(
        //         new FlatMapFunction<Row, Row>() {
        //             @Override
        //             public Iterable<Row> call(Row row) throws Exception {
        //                 ArrayList<Row> rows = new ArrayList<>();
        //                 for (int i = 0; i < 10; i++) {
        //                     rows.add(RowFactory.create(
        //                             i + "_" + row.getLong(0),
        //                             row.getString(1),
        //                             row.getString(2)
        //                     ));
        //                 }
        //                 return rows;
        //             }
        //         }
        // );
        //
        // StructType tempSchema = DataTypes.createStructType(Arrays.asList(
        //         DataTypes.createStructField("product_id", DataTypes.StringType, true),
        //         DataTypes.createStructField("product_name", DataTypes.StringType, true),
        //         DataTypes.createStructField("extend_info", DataTypes.StringType, true)
        // ));
        // DataFrame tempDf = sqlContext.createDataFrame(tempProductInfoRDD, tempSchema);
        // tempDf.registerTempTable("tmp_product_info");
        //
        // String sql = "select " +
        //         "tapcc.area," +
        //         "string_to_long(remove_random_prefix(tapcc.product_id)) product_id," +
        //         "tapcc.click_count," +
        //         "tapcc.city_infos," +
        //         "tpi.product_name," +
        //         "if(get_json_object(tpi.extend_info,'product_status')='0','Self','Third Party') product_status " +
        //         "from " +
        //         "(" +
        //             "select " +
        //             "area," +
        //             "random_prefix(long_to_string(product_id),10) product_id," +
        //             "click_count,city_infos " +
        //             "from " +
        //             "tmp_area_product_click_count" +
        //         ") tapcc " +
        //         "join " +
        //         "tmp_product_info tpi " +
        //         "on " +
        //         "tapcc.product_id = tpi.product_id";

        DataFrame df = sqlContext.sql(sql);

        System.out.println("tmp_area_fullprod_click_count: " + df.count());
        System.out.println(df.javaRDD().collect());

        df.registerTempTable("tmp_area_fullprod_click_count");
    }

    /*
     * Author: XuKai
     * Description: 获取各区域top3热门商品
     * Created: 2018/5/24 0:38
     * Params: [sqlContext]
     * Returns: org.apache.spark.api.java.JavaRDD<org.apache.spark.sql.Row>
     */
    private static JavaRDD<Row> getAreaTop3ProductRDD(SQLContext sqlContext) {

        // 技术点：开窗函数

        // 使用开窗函数先进行一个子查询
        // 按照area进行分组，给每个分组内的数据，按照点击次数降序排序，打上一个组内的行号
        // 接着在外层查询中，过滤出各个组内的行号排名前3的数据
        // 其实就是咱们的各个区域下top3热门商品

        // 华北、华东、华南、华中、西北、西南、东北
        // A级：华北、华东
        // B级：华南、华中
        // C级：西北、西南
        // D级：东北

        // case when
        // 根据多个条件，不同的条件对应不同的值
        // case when then ... when then ... else ... end

        String sql = "select " +
                "area," +
                "case " +
                "when area='North' or area='East' then 'A Level' " +
                "when area='South' or area='Middle' then 'B Level' " +
                "when area='North West' or area='North East' then 'C Level' " +
                "else 'D Level' " +
                "end area_level," +
                "product_id," +
                "city_infos," +
                "click_count," +
                "product_name," +
                "product_status " +
                "from " +
                "(select " +
                "area," +
                "product_id," +
                "city_infos," +
                "click_count," +
                "product_name," +
                "product_status," +
                "row_number() over (partition by area order by click_count desc) rank " +
                "from " +
                "tmp_area_fullprod_click_count) t " +
                "where rank<=3";
        DataFrame df = sqlContext.sql(sql);
        return df.javaRDD();
    }

    /*
     * Author: XuKai
     * Description: 将计算出来的各区域top3热门商品写入MySQL中
     * Created: 2018/5/24 11:26
     * Params: [taskid, rows]
     * Returns: void
     */
    private static void persistAreaTop3Product(long taskid, List<Row> rows) {
        IAreaTop3ProductDAO areaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
        ArrayList<AreaTop3Product> areaTop3Products = new ArrayList<>();
        for (Row row : rows) {
            areaTop3Products.add(new AreaTop3Product(
                    taskid,
                    row.getString(0),
                    row.getString(1),
                    row.getLong(2),
                    row.getString(3),
                    row.getLong(4),
                    row.getString(5),
                    row.getString(6)
            ));
        }
        areaTop3ProductDAO.insert(areaTop3Products);
    }

}