package com.realldy.spark.core.rdd.persist

import org.apache.spark.rdd.RDD

object Spark01_RDD_Persist {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Wordcount").set("spark.testing.memory", "512000000")

    val sc = new SparkContext(conf)

    //    读取文件
    val line = sc.textFile("word.txt")

    //    将一行数据进行拆分
    val words = line.flatMap(_.split(" "))

    //单词计数
    val wordNum = words.map {
      println("------------")
      (_, 1)
    }

    //统计
    val wordToCount = wordNum.reduceByKey(_ + _)
    //    对分组后的数据进行转换
    val arr = wordToCount.collect()
    arr.foreach(println)

    //分组
    //rdd不存储数据，从最开始运行
    val groupRDD: RDD[(String, Iterable[Int])] = wordNum.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }


}
