package com.learn.analyze.test;

import com.learn.analyze.constant.Constants;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Author: Xukai
 * Description:
 * CreateDate: 2018/5/2 14:06
 * Modified By:
 */
public class TestHiveSql {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(Constants.SPARK_APP_NAME_PAGE)
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc);
        DataFrame resultHiveDF = hiveContext.sql("select * from sparkanalyze.user_visit_action limit 5");
        JavaRDD<Row> rowJavaRDD = resultHiveDF.javaRDD();
        for (Row row : rowJavaRDD.collect()) {
            System.out.println(row);
        }
    }
}
