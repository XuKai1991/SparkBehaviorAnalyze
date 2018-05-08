package com.learn.sparkanalyze.util;

import com.alibaba.fastjson.JSONObject;
import com.learn.sparkanalyze.conf.ConfigurationManager;
import com.learn.sparkanalyze.constant.Constants;
import com.learn.sparkanalyze.test.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import static com.learn.sparkanalyze.constant.Constants.SPARK_LOCAL;

/**
 * Spark工具类
 *
 * @author Administrator
 */
public class SparkUtils {

    /*
     * Author: XuKai
     * Description: 根据当前是否本地测试的配置决定，如何设置SparkConf的master
     * Created: 2018/4/28 11:03
     * Params: [conf]
     */
    public static void setMaster(SparkConf conf) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            conf.setMaster("local");
        }
    }

    /*
     * Author: XuKai
     * Description: 获取SQLContext
     * 如果是在本地测试环境的话，那么生成SQLContext对象
     * 如果是在生产环境运行的话，那么生成HiveContext对象
     * Created: 2018/4/28 1:00
     * Params: [sc]
     */
    public static SQLContext getSqlContext(SparkContext sc) {
        Boolean isLocal = ConfigurationManager.getBoolean(SPARK_LOCAL);
        if (isLocal) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }

    /*
     * Author: XuKai
     * Description: 生成模拟数据（只有本地模式，才会生成模拟数据）
     * Created: 2018/4/28 9:11
     * Params: [sc, sqlContext]
     */
    public static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        Boolean isLocal = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (isLocal) {
            MockData.mock(sc, sqlContext);
        }
    }

    /*
     * Author: XuKai
     * Description: 获取指定日期范围内的用户访问行为数据RDD
     * Created: 2018/4/28 10:37
     * Params: [sqlContext, taskParam 任务参数]
     */
    public static JavaRDD<Row> getActionRddByDateRange(SQLContext sqlContext, JSONObject taskParam) {
        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
        String sql = "select * from user_visit_action where" +
                " date>='" + startDate + "'" +
                " and date<='" + endDate + "'";
//				+ "and session_id not in('','','')"
        DataFrame actionDff = sqlContext.sql(sql);

        /**
         * 这里就很有可能发生上面说的问题
         * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
         * 实际上，你需要1000个task去并行执行
         *
         * 所以说，在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
         */

//		return actionDF.javaRDD().repartition(1000);

        return actionDff.javaRDD();
    }

}
