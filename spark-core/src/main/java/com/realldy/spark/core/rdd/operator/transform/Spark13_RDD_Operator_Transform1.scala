package com.realldy.spark.core.rdd.operator.transform

object Spark13_RDD_Operator_Transform1 {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd1 = sc.makeRDD(List(1,2,3,4),2)
    val rdd2 = sc.makeRDD(List(3,4,5,6),4)

    //TODO 拉链
    //(1,3),(2,4),(3,5),(4,6)
    val rdd6 = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    //TODO 关闭环境
    sc.stop()
  }


}
