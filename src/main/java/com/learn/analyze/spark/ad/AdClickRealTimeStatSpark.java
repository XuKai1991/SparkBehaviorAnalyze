package com.learn.analyze.spark.ad;

import com.google.common.base.Optional;
import com.learn.analyze.conf.ConfigurationManager;
import com.learn.analyze.constant.Constants;
import com.learn.analyze.dao.*;
import com.learn.analyze.dao.factory.DAOFactory;
import com.learn.analyze.domain.AdClickTrend;
import com.learn.analyze.domain.AdProvinceTop3;
import com.learn.analyze.domain.AdStat;
import com.learn.analyze.domain.AdUserClickCount;
import com.learn.analyze.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Author: Xukai
 * Description: 广告点击流量实时统计spark作业
 * CreateDate: 2018/5/25 11:00
 * Modified By:
 */
public class AdClickRealTimeStatSpark {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 构建Spark Streaming上下文
        SparkConf conf = new SparkConf()
                // 使用Kryo序列化机制
                // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                // 设置默认并行度
                // .set("spark.default.parallelism", "1000")
                // 减小block interval使每个batch包含更多block，表示得到更多partition，增加处理并行度
                // .set("spark.streaming.blockInterval", "50")
                // 启动WAL预写日志机制，实现RDD高可用性
                // .set("spark.streaming.receiver.writeAheadLog.enable", "true")
                .setAppName("AdClickRealTimeStatSpark");
        // SparkUtils.setMaster(conf);
        conf.setMaster("local[2]");  // 用于本地测试HiveSql

        // spark streaming的上下文是构建JavaStreamingContext对象
        // 而不是像之前的JavaSparkContext、SQLContext/HiveContext
        // 第一个参数，和之前的spark上下文一样，也是SparkConf对象
        // 第二个参数是spark streaming类型作业比较有特色的一个参数：实时处理batch的interval
        // spark streaming，每隔一小段时间，会去收集一次数据源（kafka）中的数据，做成一个batch
        // 每次都是处理一个batch中的数据

        // 通常batch interval是指每隔多少时间收集一次数据源中的数据，然后进行处理
        // 一般的spark streaming应用，都是设置数秒到数十秒（很少会超过1分钟）

        // 本项目中，就设置5秒钟的batch interval
        // 每隔5秒钟，spark streaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 设置checkpoint目录,有些会自动进行checkpoint操作的DStream，实现HA高可用性
        jssc.checkpoint("hdfs://192.168.25.150:9000/streaming_checkpoint");

        // 业务逻辑

        // 构建kafka参数map，放置要连接的kafka集群的地址（broker集群的地址列表）
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        kafkaParams.put("group.id", ConfigurationManager.getProperty(Constants.KAFKA_GROUP_ID));
        kafkaParams.put("auto.offset.reset", ConfigurationManager.getProperty(Constants.KAFKA_AUTO_OFFSET_RESET));

        // 构建topic set
        String topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        HashSet<String> topicsSet = new HashSet<>();
        for (String topic : topics.split(",")) {
            topicsSet.add(topic);
        }

        // 基于kafka direct api模式，构建出针对kafka集群中指定topic的输入DStream
        // 结果DStream包含两个值，val1，val2
        // val1没有什么特殊的意义，val2中包含了kafka topic中的一条一条的实时日志数据
        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        // 重分区，增加每个batch rdd的partition数量
        // adRealTimeLogDStream.repartition(1000);

        // 根据动态黑名单进行数据过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream);

        // 生成动态黑名单
        generateDynamicBlacklist(filteredAdRealTimeLogDStream);

        // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
        // 最粗
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream);

        // 业务功能二：实时统计每天每个省份top3热门广告
        // 统计的稍微细一些了
        calculateProvinceTop3Ad(adRealTimeStatDStream);

        // 业务功能三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
        // 统计的非常细了
        // 每次都可以看到每个广告，最近一小时内，每分钟的点击量
        // 每支广告的点击趋势
        calculateAdClickCountByWindow(filteredAdRealTimeLogDStream);

        // 构建完spark streaming上下文之后，记得要进行上下文的启动、等待执行结束、关闭
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

    /*
     * Author: XuKai
     * Description: 根据动态黑名单进行数据过滤
     * Created: 2018/5/25 12:10
     * Params: [adRealTimeLogDStream]
     * Returns: org.apache.spark.streaming.api.java.JavaPairDStream<java.lang.String,java.lang.String>
     */
    private static JavaPairDStream<String, String> filterByBlacklist(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
        // 刚刚接受到原始的用户点击行为日志后
        // 根据mysql中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
        // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(
                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                    private static final long serialVersionUID = -7143123487887647285L;

                    @Override
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {

                        // 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd
                        IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                        List<Long> adBlacklists = adBlacklistDAO.findAll();
                        List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();
                        for (Long userid : adBlacklists) {
                            tuples.add(new Tuple2<Long, Boolean>(userid, true));
                        }
                        // 从rdd获得上下文
                        JavaSparkContext sc = new JavaSparkContext(rdd.context());
                        // 并行化成RDD
                        JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);

                        // 将原始数据rdd映射成<userid, tuple2<string, string>>
                        JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(
                                new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {
                                    private static final long serialVersionUID = 6035713611687640146L;

                                    @Override
                                    public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple) throws Exception {
                                        String[] splitedLog = tuple._2.split(" ");
                                        Long userid = Long.valueOf(splitedLog[3]);
                                        return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
                                    }
                                }
                        );

                        // 将原始日志数据rdd，与黑名单rdd，进行左外连接
                        // 如果原始日志的userid，没有在对应的黑名单中，join不到，所以用左外连接
                        // 如果用inner join，内连接，会导致数据丢失
                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD);

                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(
                                new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {

                                    private static final long serialVersionUID = 1797777199891556037L;

                                    @Override
                                    public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                                        Optional<Boolean> booleanOptional = tuple._2._2;
                                        // 如果这个值存在，那么说明原始日志中的userid，join到了某个黑名单用户
                                        if (booleanOptional.isPresent() && booleanOptional.get()) {
                                            return false;
                                        }
                                        return true;
                                    }
                                }
                        );
                        JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(
                                new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
                                    private static final long serialVersionUID = -9012569846237008570L;

                                    @Override
                                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple) throws Exception {
                                        return tuple._2._1;
                                    }
                                }
                        );
                        return resultRDD;
                    }
                }
        );
        return filteredAdRealTimeLogDStream;
    }

    /*
     * Author: XuKai
     * Description: 生成动态黑名单
     * Created: 2018/5/25 12:11
     * Params: [filteredAdRealTimeLogDStream]
     * Returns: void
     */
    private static void generateDynamicBlacklist(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // 计算每5秒中，每天每个用户每个广告的点击量
        // 通过对原始实时日志处理，得到数据格式为<yyyyMMdd_userid_adid, 1L>
        JavaPairDStream<String, Long> dailyUserAdClickDstream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {

                    private static final long serialVersionUID = -1495226170842208146L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                        // 从tuple中获取每一条原始的实时日志
                        String[] splitedLog = tuple._2.split(" ");

                        // 提取日期
                        String timestamp = splitedLog[0];
                        Date date = new Date(Long.valueOf(timestamp));
                        String dateKey = DateUtils.formatDateKey(date);

                        Long userid = Long.valueOf(splitedLog[3]);
                        Long adid = Long.valueOf(splitedLog[4]);

                        return new Tuple2<String, Long>(dateKey + "_" + userid + "_" + adid, 1L);
                    }
                }
        );

        // 针对处理后的日志格式，执行reduceByKey算子
        // （每个batch中）每天每个用户对每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDstream.reduceByKey(
                new Function2<Long, Long, Long>() {
                    private static final long serialVersionUID = -2879751696237462999L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );
        // 到这里为止，获取到数据dailyUserAdClickCountDStream DStream
        // 这是源源不断的，每个5s的batch中，当天每个用户对每支广告的点击次数
        // <yyyyMMdd_userid_adid, clickCount>
        dailyUserAdClickCountDStream.foreachRDD(
                new Function<JavaPairRDD<String, Long>, Void>() {
                    private static final long serialVersionUID = -3820860445458701097L;

                    @Override
                    public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                                    private static final long serialVersionUID = -7849815725151497804L;

                                    @Override
                                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                                        // 对每个分区的数据就去获取一次连接对象
                                        // 每次都是从连接池中获取，而不是每次都创建
                                        // 写数据库操作，性能已经提到最高了
                                        ArrayList<AdUserClickCount> adUserClickCounts = new ArrayList<>();
                                        while (iterator.hasNext()) {
                                            Tuple2<String, Long> tuple = iterator.next();
                                            String[] splitedKey = tuple._1.split("_");
                                            // 字符串yyyyMMdd -> Date -> 字符串yyyy-MM-dd
                                            String date = null;
                                            date = DateUtils.formatDate(DateUtils.parseDateKey(splitedKey[0]));
                                            Long userid = Long.valueOf(splitedKey[1]);
                                            Long adid = Long.valueOf(splitedKey[2]);
                                            Long clickCount = tuple._2;
                                            adUserClickCounts.add(new AdUserClickCount(date, userid, adid, clickCount));
                                        }
                                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                                        adUserClickCountDAO.updateBatch(adUserClickCounts);
                                    }
                                }
                        );
                        return null;
                    }
                }
        );

        // 现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量
        // 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
        // 从mysql中查询
        // 查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
        // 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化

        // 对batch中的数据，去查询mysql中的点击次数，使用哪个dstream呢？
        // dailyUserAdClickCountDStream
        // 为什么用这个batch？因为这个batch是聚合过的数据，已经按照yyyyMMdd_userid_adid进行过聚合了
        // 比如原始数据可能是一个batch有一万条，聚合过后可能只有五千条
        // 所以选用这个聚合后的dstream，既可以满足需求，而且还可以尽量减少要处理的数据量

        JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(
                new Function<Tuple2<String, Long>, Boolean>() {
                    private static final long serialVersionUID = -9054127811607749507L;

                    @Override
                    public Boolean call(Tuple2<String, Long> tuple) throws Exception {
                        // <yyyyMMdd_userid_adid, clickCount>
                        String[] splitedKey = tuple._1.split("_");
                        String date = null;
                        date = DateUtils.formatDate(DateUtils.parseDateKey(splitedKey[0]));
                        Long userid = Long.valueOf(splitedKey[1]);
                        Long adid = Long.valueOf(splitedKey[2]);

                        // 从mysql中查询指定日期指定用户对指定广告的点击量
                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                        int clickCount = adUserClickCountDAO.findClickCountByMultiKey(date, userid, adid);
                        // 判断，如果点击量大于等于100，就拉入黑名单，返回true
                        if (clickCount >= 100) {
                            return true;
                        }
                        return false;
                    }
                }
        );

        // blacklistDStream
        // 里面的每个batch，都是过滤出来的已经在某天对某个广告点击量超过100的用户
        // 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
        // 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
        // 我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
        // 所以直接插入mysql即可

        // 这里有个问题
        // blacklistDStream中，可能有userid是重复的，如果直接插入就会插入重复的黑明单用户
        // 所以需要在插入前要进行去重
        // yyyyMMdd_userid_adid
        // 20151220_10001_10002 100
        // 20151220_10001_10003 100
        // 10001这个userid就重复了
        JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(
                new Function<Tuple2<String, Long>, Long>() {
                    private static final long serialVersionUID = -4588520847034802709L;

                    @Override
                    public Long call(Tuple2<String, Long> tuple) throws Exception {
                        // <yyyyMMdd_userid_adid, clickCount>
                        String[] splitedKey = tuple._1().split("_");
                        Long userid = Long.valueOf(splitedKey[1]);
                        return userid;
                    }
                }
        );
        // 使用transform算子（将dstream中的每个batch RDD进行处理，转换为任意的其他RDD，功能很强大）
        JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(
                new Function<JavaRDD<Long>, JavaRDD<Long>>() {
                    private static final long serialVersionUID = 4449743708541266495L;

                    @Override
                    public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
                        return rdd.distinct();
                    }
                }
        );
        // 到这一步为止，distinctBlacklistUseridDStream
        // 每一个rdd，只包含了userid，而且还进行了全局的去重，保证每一次过滤出来的黑名单用户都没有重复的
        // 遍历这个Dstream，将黑名单的userid写入mysql
        distinctBlacklistUseridDStream.foreachRDD(
                new Function<JavaRDD<Long>, Void>() {
                    private static final long serialVersionUID = 2710388563370225182L;

                    @Override
                    public Void call(JavaRDD<Long> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Long>>() {
                                    private static final long serialVersionUID = -6922162165047251781L;

                                    @Override
                                    public void call(Iterator<Long> iterator) throws Exception {
                                        List<Long> adBlackList = new ArrayList<Long>();
                                        while (iterator.hasNext()) {
                                            Long userid = iterator.next();
                                            adBlackList.add(userid);
                                        }
                                        IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                                        adBlacklistDAO.insertBatch(adBlackList);

                                        // 到此为止，实现了动态黑名单
                                        // 1、计算出每个batch中的每天每个用户对每个广告的点击量，并持久化到mysql中

                                        // 2、依据上述计算出来的数据，对每个batch中的按date、userid、adid聚合的数据
                                        // 都要遍历一遍，查询对应的累计的点击次数，如果超过100，就认定为黑名单
                                        // 然后对黑名单用户进行去重，去重后将黑名单用户持久化插入到mysql中
                                        // 所以mysql中的ad_blacklist表中的黑名单用户是动态地实时地增长的
                                        // mysql中的ad_blacklist表，是一张动态黑名单

                                        // 3、基于上述计算出来的动态黑名单，在最一开始，就对每个batch中的点击行为
                                        // 根据动态黑名单进行过滤
                                        // 把黑名单中的用户的点击行为，直接过滤掉

                                        // 动态黑名单机制，就完成了
                                    }
                                }
                        );
                        return null;
                    }
                }
        );
    }


    /*
     * Author: XuKai
     * Description: 计算广告点击流量实时统计
     * Created: 2018/5/25 12:06
     * Params: [adRealTimeLogDStream]
     * Returns: org.apache.spark.streaming.api.java.JavaPairDStream<java.lang.String,java.lang.Long>
     */
    private static JavaPairDStream<String, Long> calculateRealTimeStat(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // 业务逻辑一
        // 广告点击流量实时统计
        // 上面的黑名单实际上是广告类的实时系统中，比较常见的一种基础的应用
        // 实际上的业务功能，并不只是黑名单

        // 计算每天各省各城市各广告的点击量
        // 这份数据，实时不断地更新到mysql中的，J2EE系统提供实时报表给用户查看
        // j2ee系统每隔几秒钟，就从mysql中取一次最新数据，每次都可能不一样
        // 设计出来几个维度：日期、省份、城市、广告
        // j2ee系统就可以非常的灵活
        // 用户可以看到，实时的数据，比如2015-11-01，历史数据
        // 2015-12-01当天，可以看到当天所有的实时数据（动态改变），比如江苏省南京市
        // 广告可以进行选择（广告主、广告名称、广告类型来筛选一个出来）
        // 拿着date、province、city、adid，去mysql中查询最新的数据
        // 等等，基于这几个维度，以及这份动态改变的数据，是可以实现比较灵活的广告点击流量查看的功能的

        // 源数据格式：date province city userid adid
        // date_province_city_adid，作为key；1作为value
        // 通过spark，直接统计出来全局的点击次数，在spark集群中保留一份；在mysql中，也保留一份
        // 对原始数据进行map，映射成<date_province_city_adid,1>格式
        // 然后对上述格式的数据，执行updateStateByKey算子
        // spark streaming特有的一种算子，在spark集群内存中，维护一份key的全局状态
        JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    private static final long serialVersionUID = 6272944886057681543L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                        // <date_province_city_adid, clickCount>
                        String[] splitedLog = tuple._2.split(" ");

                        // 提取日期
                        String timestamp = splitedLog[0];
                        Date date = new Date(Long.valueOf(timestamp));
                        String dateKey = DateUtils.formatDateKey(date);

                        String province = splitedLog[1];
                        String city = splitedLog[2];
                        Long adid = Long.valueOf(splitedLog[4]);
                        return new Tuple2<String, Long>(dateKey + "_" + province + "_" + city + "_" + adid, 1L);
                    }
                }
        );
        // 在这个dstream中，就相当于，有每个batch rdd累加的各个key（各天各省份各城市各广告的点击次数）
        // 每次计算出最新的值，就在aggregatedDStream中的每个batch rdd中反应出来
        JavaPairDStream<String, Long> aggregatedDStream = mappedDStream.updateStateByKey(
                new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
                    private static final long serialVersionUID = -930170805427370765L;

                    @Override
                    public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
                        // 举例：
                        // 对于每个key，都会调用一次这个方法
                        // 比如key是<20151201_Jiangsu_Nanjing_10001,1>，就会调用一次这个方法
                        // 10个  =>   <values，(1,1,1,1,1,1,1,1,1,1)>

                        // 首先根据optional判断，之前这个key，是否有对应的状态
                        long clickCount = 0L;

                        // 如果说，之前是存在这个状态的，那么就以之前的状态作为起点，进行值的累加
                        if (optional.isPresent()) {
                            clickCount = optional.get();
                        }
                        // values，代表了，batch rdd中，每个key对应的所有的值
                        for (Long value : values) {
                            clickCount += value;
                        }
                        return Optional.of(clickCount);
                    }
                }
        );
        // 由于updateStateByKey算子做全局累加操作，所以历史数据会一直保留在DStream中
        // 这就给mysql数据库增加了写的压力，刚运行的前期，只会累计几天的数据，数据量还比较小
        // 运行很长时间后，DStream中保留了大量历史数据，每次写当天的数据时都会把历史数据也遍历一遍，然后重新写到数据库里
        // 而事实上历史数据已经不会再变化了，相当于做无用功，还浪费资源
        // 所以此处对全局累加结果进行过滤，只保留当天数据
        // JavaPairDStream<String, Long> filtedAggregatedDStream = aggregatedDStream.filter(
        //         new Function<Tuple2<String, Long>, Boolean>() {
        //             @Override
        //             public Boolean call(Tuple2<String, Long> tuple) throws Exception {
        //                 // 数据格式<date_province_city_adid, clickCount>
        //                 String[] splitedKey = tuple._1.split("_");
        //                 String dateKey = splitedKey[0];
        //                 String todayKey = DateUtils.formatDateKey(new Date());
        //                 return dateKey.equals(todayKey);
        //             }
        //         }
        // );
        // 将计算出来的最新结果，同步一份到mysql中，以便于j2ee系统使用
        aggregatedDStream.foreachRDD(
                new Function<JavaPairRDD<String, Long>, Void>() {
                    @Override
                    public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                                    private static final long serialVersionUID = 8978562965585097819L;

                                    @Override
                                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                                        ArrayList<AdStat> adStats = new ArrayList<>();
                                        while (iterator.hasNext()) {
                                            // <date_province_city_adid, clickCount>
                                            Tuple2<String, Long> tuple = iterator.next();
                                            String[] splitedKey = tuple._1.split("_");
                                            String date = DateUtils.formatDate(DateUtils.parseDateKey(splitedKey[0]));
                                            String province = splitedKey[1];
                                            String city = splitedKey[2];
                                            long adid = Long.valueOf(splitedKey[3]);
                                            Long clickCount = tuple._2;
                                            adStats.add(new AdStat(date, province, city, adid, clickCount));
                                        }
                                        IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
                                        adStatDAO.updateBatch(adStats);
                                    }
                                }
                        );
                        return null;
                    }
                }
        );
        return aggregatedDStream;
    }

    /*
     * Author: XuKai
     * Description: 计算每天各省份的top3热门广告
     * Created: 2018/5/28 12:00
     * Params: [adRealTimeStatDStream]
     * Returns: void
     */
    private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {
        // adRealTimeStatDStream
        // 每一个batch rdd，都代表最新的全量的每天各省份各城市各广告的点击量
        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform(
                new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
                    private static final long serialVersionUID = 4661713927038657580L;

                    @Override
                    public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
                        // <yyyyMMdd_province_city_adid, clickCount> 映射成 <yyyyMMdd_province_adid, clickCount>
                        JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(
                                new PairFunction<Tuple2<String, Long>, String, Long>() {
                                    private static final long serialVersionUID = -4052733177191406022L;

                                    @Override
                                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {
                                        String[] splitedKey = tuple._1.split("_");
                                        return new Tuple2<>(splitedKey[0] + "_" + splitedKey[1] + "_" + splitedKey[3], tuple._2);
                                    }
                                }
                        );
                        // 计算出每天各省份各广告的点击量
                        JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(
                                new Function2<Long, Long, Long>() {
                                    private static final long serialVersionUID = -8173406849364287294L;

                                    @Override
                                    public Long call(Long v1, Long v2) throws Exception {
                                        return v1 + v2;
                                    }
                                }
                        );
                        // 将dailyAdClickCountByProvinceRDD转换为DataFrame
                        // 注册为一张临时表
                        // 使用Spark SQL，通过开窗函数，获取每天各省份的top3热门广告
                        JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(
                                new Function<Tuple2<String, Long>, Row>() {
                                    private static final long serialVersionUID = 6704167506419947157L;

                                    @Override
                                    public Row call(Tuple2<String, Long> tuple) throws Exception {
                                        String[] splitedKey = tuple._1.split("_");
                                        String date = DateUtils.formatDate(DateUtils.parseDateKey(splitedKey[0]));
                                        String province = splitedKey[1];
                                        long adid = Long.valueOf(splitedKey[2]);
                                        long clickCount = tuple._2;
                                        return RowFactory.create(date, province, adid, clickCount);
                                    }
                                }
                        );
                        StructType schema = DataTypes.createStructType(Arrays.asList(
                                DataTypes.createStructField("date", DataTypes.StringType, true),
                                DataTypes.createStructField("province", DataTypes.StringType, true),
                                DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                                DataTypes.createStructField("click_count", DataTypes.LongType, true)
                        ));
                        // 通过rdd获得spark上下文context，再得到HiveContext
                        HiveContext sqlContext = new HiveContext(rdd.context());
                        // 将JavaRDD<Row>转为DataFrame，再注册成为临时表
                        DataFrame dailyAdClickCountByProvinceDF = sqlContext.createDataFrame(rowsRDD, schema);
                        dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");
                        // 使用Spark SQL执行SQL语句，配合开窗函数，统计出每天各省top3热门的广告
                        String sql = "select " +
                                "date,province,ad_id,click_count " +
                                "from " +
                                "(" +
                                "select date,province,ad_id,click_count,row_number() over (partition by date,province order by click_count desc) rank from tmp_daily_ad_click_count_by_prov" +
                                ") t" +
                                " where rank<=3";
                        DataFrame provinceTop3AdDF = sqlContext.sql(sql);
                        return provinceTop3AdDF.toJavaRDD();
                    }
                }
        );
        // rowsDStream，每次刷新出来每天各个省份最热门的top3广告
        // 将数据批量更新到MySQL中
        rowsDStream.foreachRDD(
                new Function<JavaRDD<Row>, Void>() {
                    private static final long serialVersionUID = -9027940331910342926L;

                    @Override
                    public Void call(JavaRDD<Row> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Row>>() {
                                    private static final long serialVersionUID = -2773726575885623783L;

                                    @Override
                                    public void call(Iterator<Row> iterator) throws Exception {
                                        List<AdProvinceTop3> adProvinceTop3s = new ArrayList<AdProvinceTop3>();
                                        while (iterator.hasNext()) {
                                            Row row = iterator.next();
                                            String date = row.getString(0);
                                            String province = row.getString(1);
                                            long adid = row.getLong(2);
                                            long clickCount = row.getLong(3);

                                            AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3(date, province, adid, clickCount);
                                            adProvinceTop3s.add(adProvinceTop3);
                                        }
                                        IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
                                        adProvinceTop3DAO.updateBatch(adProvinceTop3s);
                                    }
                                }
                        );
                        return null;
                    }
                }
        );
    }

    /*
     * Author: XuKai
     * Description: 计算最近1小时内每分钟广告点击趋势
     * Created: 2018/5/28 12:02
     * Params: [adRealTimeLogDStream]
     * Returns: void
     */
    private static void calculateAdClickCountByWindow(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // 映射成<yyyyMMddHHMM_adid,1L>格式
        JavaPairDStream<String, Long> pairDStream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    private static final long serialVersionUID = -4529233504159688702L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                        // 从tuple中获取每一条原始的实时日志
                        String[] splitedLog = tuple._2.split(" ");

                        // 提取日期，转化为yyyyMMddHHMM
                        String timestamp = splitedLog[0];
                        Date date = new Date(Long.valueOf(timestamp));
                        String timeMinute = DateUtils.formatTimeMinute(date);

                        Long adid = Long.valueOf(splitedLog[4]);
                        return new Tuple2<String, Long>(timeMinute + "_" + adid, 1L);
                    }
                }
        );
        // 过来的每个batch rdd，都会被映射成<yyyyMMddHHMM_adid,1L>的格式
        // 每10秒获取最近1小时内的所有的batch
        // 然后根据key进行reduceByKey操作，统计出来最近一小时内的各分钟各广告的点击次数
        // 1小时滑动窗口内的广告点击趋势，窗口滑动间隔为10s，表示每10秒刷新一次
        // 点图 / 折线图
        JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.minutes(60), Durations.seconds(10));
        // aggrRDD
        // 每次都可以拿到，最近1小时内，各分钟（yyyyMMddHHMM）各广告的点击量
        aggrRDD.foreachRDD(
                new VoidFunction<JavaPairRDD<String, Long>>() {
                    private static final long serialVersionUID = 6640134098438799480L;

                    @Override
                    public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                        rdd.foreachPartition(
                                new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                                    private static final long serialVersionUID = 5620308896042725255L;

                                    @Override
                                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                                        List<AdClickTrend> adClickTrends = new ArrayList<>();
                                        while (iterator.hasNext()) {
                                            Tuple2<String, Long> tuple = iterator.next();
                                            String[] splitedKey = tuple._1.split("_");
                                            // yyyyMMddHHmm
                                            String dateMinute = splitedKey[0];
                                            long adid = Long.valueOf(splitedKey[1]);
                                            long clickCount = tuple._2;
                                            String date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)));
                                            String hour = dateMinute.substring(8, 10);
                                            String minute = dateMinute.substring(10);
                                            adClickTrends.add(new AdClickTrend(date, hour, minute, adid, clickCount));
                                        }
                                        IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
                                        adClickTrendDAO.updateBatch(adClickTrends);
                                    }
                                }
                        );
                    }
                }
        );
    }

}












