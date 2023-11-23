package com.realldy.spark.core.req

import org.apache.spark.rdd.RDD

object test {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val iterable = List(1, 2, 3, 4, 5, 6, 7, 8)

    // 使用sliding方法创建大小为2的滑动窗口
    val slidingPairs = iterable.sliding(2).toList

    // 打印结果
    slidingPairs.foreach(pair => println(pair.mkString("(", ", ", ")")))
    //TODO 关闭环境
    sc.stop()
  }


}
