package com.realldy.spark.core.rdd.operator.action

object Spark03_RDD_Operator_Action {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //TODO aggregate算子
    //  aggregate同时参与分区内和分区间计算
    val result: Int = rdd.aggregate(10)(_ + _, _ + _)

    //TODO fold算子
    //  fold同时参与分区内和分区间计算
    val result1: Int = rdd.fold(10)(_ + _)


    println(result1)
    //TODO 关闭环境
    sc.stop()
  }


}
