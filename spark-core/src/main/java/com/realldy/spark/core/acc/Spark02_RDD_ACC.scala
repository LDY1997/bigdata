package com.realldy.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_ACC {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4), 2)

    val sumAcc = sc.longAccumulator("sum")

    //TODO foreach累计
    rdd.foreach(
      num=>{
        sumAcc.add(num)
      }
    )
    println(sumAcc.value)

    //TODO 关闭环境
    sc.stop()
  }
}