package com.realldy.spark.core.rdd.part

import org.apache.spark.{HashPartitioner, Partitioner}

object Spark01_RDD_Part {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(
      ("lol",1),
      ("csgo",2),
      ("forest",3),
      ("lol",4)
    ))


    //TODO 自定义重分区

    rdd.partitionBy(new MyPartitioner).saveAsTextFile("output/spark01_part")

    //TODO 关闭环境
    sc.stop()
  }

  class MyPartitioner extends Partitioner {
    override def numPartitions: Int = 3

    def getPartition(key : Any): Int = {
      key match {
        case "lol"=>0
        case  "csgo"=>1
        case _=>2
      }
    }
  }



}
