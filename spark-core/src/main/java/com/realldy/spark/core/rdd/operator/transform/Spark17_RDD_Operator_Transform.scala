package com.realldy.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD

object Spark17_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",3),("a",4)),2)


    //TODO aggregateByKey
    //  两个参数列表
    //  第一参数列表：分区内计算的初始值
    //  第二个参数列表：分区内计算,分区间计算
    val rdd1 = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )

    rdd1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
