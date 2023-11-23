package com.realldy.spark.core.rdd.dep

object Spark01_RDD_Dep {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val line = sc.textFile("word.txt")
    println(line.toDebugString)
    println("-----------------------------")

    //    将一行数据进行拆分
    val words = line.flatMap(_.split(" "))
    println(words.toDebugString)
    println("-----------------------------")
    //    将单词进行分组
    val groupWord = words.groupBy(t => t)
    println(groupWord.toDebugString)
    println("-----------------------------")
    //    数据根据单词进行分组，便于统计
    val wordToCount = groupWord.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    println(wordToCount.toDebugString)
    println("-----------------------------")

    //    对分组后的数据进行转换
    val arr = wordToCount.collect()
    arr.foreach(println)


    //TODO 关闭环境
    sc.stop()
  }


}
