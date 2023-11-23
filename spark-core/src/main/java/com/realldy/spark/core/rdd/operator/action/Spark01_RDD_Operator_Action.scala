package com.realldy.spark.core.rdd.operator.action

object Spark01_RDD_Operator_Action {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //TODO foreach算子
    //  在executor端打印数据
    rdd.foreach(println)
    println("-------------------------")
    //  在drive端打印数据
    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
