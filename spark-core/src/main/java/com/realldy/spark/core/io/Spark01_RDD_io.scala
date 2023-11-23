package com.realldy.spark.core.io

import org.apache.spark.api.java.JavaSparkContext.fromSparkContext

object Spark01_RDD_io {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(("a",1),("a",2),("a",1),("b",2)))


//    //TODO 保存
//    rdd.saveAsTextFile("output/io1")
//    rdd.saveAsObjectFile("output/io2")
//    rdd.saveAsSequenceFile("output/io3")


    //TODO 读取
    val rdd1 = sc.textFile("output/io1")
    println(rdd1.collect().mkString(","))

    val rdd2 = sc.objectFile[(String, Int)]("output/io2")
    println(rdd2.collect().mkString(","))

    val rdd3 = sc.sequenceFile[String, Int]("output/io3")
    println(rdd3.collect().mkString(","))

    //TODO 关闭环境
    sc.stop()
  }


}
