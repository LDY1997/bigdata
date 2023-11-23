package com.realldy.spark.core.rdd.builder

object Spark01_RDD_File {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从文件创建RDD
    //1.从文件
    val rdd = sc.textFile("data/word.txt")
    //2.从文件夹
    val rdd1 = sc.textFile("data")
    //3.使用通配符
    val rdd2 = sc.textFile("data/w*")
    //4.用wholeTextFiles。区别：textFile按行读取，wholeTextFiles按文件读取
    val rdd3 = sc.wholeTextFiles("data")

    rdd3.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
