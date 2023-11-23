package com.realldy.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD

object Spark16_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",1),("b",2)))


    //TODO groupbykey:将数据源的数据，相同的分数放在一个分组，形成一个队都
    //  注意groubykey和grouby返回的类型是不一样的
    val rdd1: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val rdd2: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    rdd1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
