package com.realldy.spark.core.rdd.operator.transform

object Spark01_RDD_Operator_Transform_Test {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.textFile("data/apache.log")
    val resRdd = rdd.map(_.split(" ")(6))

    resRdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
