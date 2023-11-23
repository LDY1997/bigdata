package com.realldy.spark.core.rdd.operator.transform

import org.apache.spark.HashPartitioner

object Spark14_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4))


    //TODO 哈希重分区，如果分区方法和数量一样,spark不会进行分区操作
    //首先需要转(k,v)
    val rdd1 = rdd.map((_, 1))
    //重分区
    rdd1.partitionBy(new HashPartitioner(2)).saveAsTextFile("output/spark14")

    //TODO 关闭环境
    sc.stop()
  }


}
