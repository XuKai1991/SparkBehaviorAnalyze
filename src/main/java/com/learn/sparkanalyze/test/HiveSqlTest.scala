package com.learn.sparkanalyze.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Xukai
  * Description: 
  * CreateDate: 2018/5/22 1:22
  * Modified By: 
  */
object HiveSqlTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val conf = new SparkConf().setAppName("HiveSqlTest").setMaster("local")
    val sc = new SparkContext(conf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val resultHiveDF = hiveContext.sql("select * from sparkanalyze.user_visit_action limit 5")
    resultHiveDF.show()
    sc.stop()
  }

}
