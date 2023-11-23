package com.realldy.spark.core.rdd.operator.transform

object Spark02_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //TODO mapPartitions函数
    //     mapPartitions可以以分区为单位对数据进行批处理，但会将整个分区拉到内存中。
    val makeRdd1 = rdd.mapPartitions(
      iter => {
        println("----------")
        iter.map(_ * 2)
      }
    )

    makeRdd1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
