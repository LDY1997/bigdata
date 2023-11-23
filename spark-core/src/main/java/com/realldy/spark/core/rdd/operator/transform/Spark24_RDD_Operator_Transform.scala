package com.realldy.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD

object Spark24_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.textFile("data/agent.log")
    //TODO cogroup
    val rdd1: RDD[(String, List[(String, Int)])] = rdd
      .map {
        line => {
          val word = line.split(" ")
          ((word(1), word(4)), 1)
        }
      }.reduceByKey(_ + _)
      .map {
        case (tuple, i) => {
          (tuple._1, (tuple._2, i))
        }
      }
      .groupByKey()
      .mapValues {
        iter => {
          iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
        }
      }
    rdd1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
