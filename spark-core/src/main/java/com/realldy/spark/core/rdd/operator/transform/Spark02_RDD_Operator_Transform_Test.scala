package com.realldy.spark.core.rdd.operator.transform

object Spark02_RDD_Operator_Transform_Test {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //TODO mapPartitions函数
    //     mapPartitions取每个分区最大值
    val makeRdd1 = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )

    makeRdd1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
