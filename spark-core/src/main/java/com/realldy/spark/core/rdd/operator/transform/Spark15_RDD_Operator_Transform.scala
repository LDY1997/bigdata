package com.realldy.spark.core.rdd.operator.transform

import org.apache.spark.HashPartitioner

object Spark15_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",1),("b",2)))


    //TODO reducebykey是两两聚合，且如果只有一个元素不参与计算
    //首先需要转(k,v)
    val rdd1 = rdd.reduceByKey(
      (x,y)=>{
        println(x,y)
        x+y
      }
    )
    //重分区
    rdd1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
