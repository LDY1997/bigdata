package com.realldy.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object Spark03_Req1_HotCatagoryTopAnalysis {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.textFile("data/user_visit_action.txt")
//2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_24_2019-07-17 00:01:07_null_5_39_null_null_null_null_10
    //TODO 创建并注册累加器
    val accumulator = new MyAccumulator()

    sc.register(accumulator,"wcAcc")

    //2019-07-17      38      6502cdc9-cf95-4b08-8854-f03a25baa917    24         2019-07-17 00:01:07
    // null   5,39   null,null   null,null   10

    rdd.foreach {
      line => {
        val strings = line.split("_")
        if (!strings(6).equals("null") && !strings(6).equals("-1")) {
          accumulator.add("click",strings(6))
        } else if (!strings(8).equals("null") && !strings(8).equals("-1")) {
          val orders = strings(8).split(",")
          orders.foreach(id=> accumulator.add("order",id))
        } else if (!strings(10).equals("null") && !strings(10).equals("-1")) {
          val pay = strings(10).split(",")
          pay.foreach(id=> accumulator.add("pay",id))
        }else{
          Nil
        }
      }
    }
    val acc2 = accumulator.value.map(_._2).toList
    val categories = acc2.sortBy {
      case (hc) =>
        (-hc.clickCount, -hc.orderCount, -hc.payCount) // 降序排列，使用负数表示降序
    }.take(10).foreach(println)

    //TODO 关闭环境
    sc.stop()
  }

  case class HotCategory(actionType:String,var clickCount:Int,var orderCount:Int,var payCount:Int)

  class MyAccumulator extends AccumulatorV2[(String,String), mutable.Map[String,HotCategory]] {
    private val wcMap = mutable.Map[String, HotCategory]()

    //判断初始值
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    //复制累加器
    override def copy(): AccumulatorV2[(String,String), mutable.Map[String,HotCategory]] = {
      new MyAccumulator()
    }

    //重置类
    override def reset(): Unit = {
      wcMap.clear()
    }

    override def add(v: (String,String)): Unit = {
      val actionType = v._1
      val cid = v._2
      val hotCategory = wcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType=="click"){
        hotCategory.clickCount += 1
      }else if (actionType=="order"){
        hotCategory.orderCount += 1
      }else if (actionType=="pay"){
        hotCategory.payCount += 1
      }
      wcMap.update(cid, hotCategory)
    }

    override def merge(other: AccumulatorV2[(String,String), mutable.Map[String,HotCategory]]): Unit = {
      val wcMap1 = this.wcMap
      val wcMap2 = other.value
      wcMap2.foreach {
        case (cid, hc) => {
          val product = wcMap1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
          hc.clickCount += product.clickCount
          hc.orderCount += product.orderCount
          hc.payCount += product.payCount
          wcMap.update(cid, hc)
        }
      }

    }

    override def value: mutable.Map[String,HotCategory] = {
      wcMap
    }
  }

}
