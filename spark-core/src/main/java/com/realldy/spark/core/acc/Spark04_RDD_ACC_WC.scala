package com.realldy.spark.core.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_RDD_ACC_WC {
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
    val mapRDD = rdd.foreach(
      word=>{
        accumulator.add(word)
      }
    )
    println(accumulator.value)


    //TODO 关闭环境
    sc.stop()
  }
  class  MyAccumulator extends AccumulatorV2[String, mutable.Map[String,Long]]{
    private val wcMap: mutable.Map[String, Long] = mutable.Map[String, Long]()

    //判断初始值
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    //复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator()
    }

    //重置类
    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(v: String): Unit = {
      val count = wcMap.getOrElse(v, 0L) + 1
      wcMap.update(v,count)
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val wcMap1 = this.wcMap
      val wcMap2 = other.value
      wcMap2.foreach {
        case (k, v) => {
          val newcnt = wcMap1.getOrElse(k, 0L) + v
          wcMap1.update(k, newcnt)
        }
      }

    }

    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}