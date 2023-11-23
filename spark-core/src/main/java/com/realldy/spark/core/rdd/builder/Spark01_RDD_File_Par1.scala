package com.realldy.spark.core.rdd.builder

object Spark01_RDD_File_Par1 {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从文件创建RDD
    //  分区结果
    //  [1,2],[3],[]
    //  设置文件标号
    //  1@@ 012
    //  2@@ 345
    //  3   6
    //  spark采用HADOOP读取文件即一行读取
    //  【0，3】  [1,2]
    //  【3，6】  [3]
    //  【6，7】  []
    val rdd = sc.textFile("data/num.txt")


    rdd.saveAsTextFile("output/par_mun")

    //TODO 关闭环境
    sc.stop()
  }


}
