package com.realldy.spark.core.rdd.builder

object Spark01_RDD_File_Par {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从文件创建RDD
    //  最小分区可是设置math.min(defalutParallelism,2)
    //  分区数计算
    //  total_Size = 7（文件字节数）
    //  goal_Size = total_Size/2=3 （每个分区大小）
    //  7/3=2...1   （1.1） 大于10%创建新分区
    val rdd = sc.textFile("data/num.txt")


    rdd.saveAsTextFile("output/par_mun")

    //TODO 关闭环境
    sc.stop()
  }


}
