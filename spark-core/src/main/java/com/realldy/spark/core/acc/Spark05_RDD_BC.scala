package com.realldy.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_RDD_BC {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(("a",1),("b",2),("c",3)))

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    //替代join，减少shuffle
    rdd.map{
      case (k,v)=>{
        val l = map.getOrElse(k, 0)
        (k,(v,l))
      }
    }.collect().foreach(println)




    //TODO 关闭环境
    sc.stop()
  }

}