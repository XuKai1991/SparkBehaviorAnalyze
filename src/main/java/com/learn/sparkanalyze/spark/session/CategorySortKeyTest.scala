package com.learn.sparkanalyze.spark.session

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: Xukai
  * Description: 品类二次排序key的scala版本实现
  * CreateDate: 2018/5/7 17:40
  * Modified By: 
  */
case class CategorySortKeyTest(val clickCount: Long, val orderCount: Long, val payCount: Long) extends Ordered[CategorySortKeyTest] with Serializable {

  override def compare(that: CategorySortKeyTest): Int = {
    if (this.clickCount != that.clickCount) (this.clickCount - that.clickCount).toInt
    else if (this.orderCount != that.orderCount) (this.orderCount - that.orderCount).toInt
    else if (this.payCount != that.payCount) (this.payCount - that.payCount).toInt
    else 0
  }

}

object CategorySort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("CategorySort")
    val sc = new SparkContext(conf)
    val arr = Array(Tuple2(CategorySortKeyTest(30, 35, 40), "1"),
      Tuple2(CategorySortKeyTest(35, 30, 40), "2"),
      Tuple2(CategorySortKeyTest(30, 38, 30), "3")
    )
    val rdd = sc.parallelize(arr)
    val sortedRdd = rdd.sortByKey(false)
    println(sortedRdd.collect().toBuffer)
    sc.stop()
  }
}
