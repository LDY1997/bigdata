package com.realldy.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark07_RDD_ACC_TEST {
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List("hello","spark","hello"))

    val sumAcc = sc.longAccumulator("sum")

    val accumulator = new MyAccumulator()

    sc.register(accumulator,"wcAcc")


    //TODO foreach累计
    // 如果没有行动算子，不会执行
    val mapRDD = rdd.map(
      word=>{
        accumulator.add(word+"1")
      }
    )
    mapRDD.collect()
    accumulator.value.foreach(println)
    println(accumulator.value)


    //TODO 关闭环境
    sc.stop()
  }
  class  MyAccumulator extends AccumulatorV2[String, List[(String, String)]]{
    private var wcMap: List[(String, String)] = List.empty

    //判断初始值
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    //复制累加器
    override def copy(): AccumulatorV2[String, List[(String, String)]] = {
      new MyAccumulator()
    }

    //重置类
    override def reset(): Unit = {
      wcMap = List.empty
    }

    override def add(v: String): Unit = {
      wcMap = wcMap :+ (v, "hello")
    }

    override def merge(other: AccumulatorV2[String, List[(String, String)]]): Unit = {
      wcMap = wcMap ++ other.value

    }

    override def value: List[(String, String)] = {
      wcMap
    }
  }
}