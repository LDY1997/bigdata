package com.realldy.spark.core.req

import org.apache.spark.rdd.RDD

object Spark02_Req1_HotCatagoryTopAnalysis {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.textFile("data/user_visit_action.txt")
//2019-07-17_38_6502cdc9-cf95-4b08-8854-f03a25baa917_24_2019-07-17 00:01:07_null_5_39_null_null_null_null_10
    //TODO aggregateByKey
    //  两个参数列表
    //  第一参数列表：分区内计算的初始值
    //  第二个参数列表：分区内计算,分区间计算
    //2019-07-17      38      6502cdc9-cf95-4b08-8854-f03a25baa917    24         2019-07-17 00:01:07
    // null   5,39   null,null   null,null   10

    val rdd1 = rdd.flatMap {
      line => {
        val strings = line.split("_")
        if (!strings(6).equals("null") && !strings(6).equals("-1")) {
          List((strings(6), (1, 0, 0)))
        } else if (!strings(8).equals("null") && !strings(8).equals("-1")) {
          val orders = strings(8).split(",")
          orders.map(id=>(id,(0, 1, 0)))
        } else if (!strings(10).equals("null") && !strings(10).equals("-1")) {
          val pay = strings(10).split(",")
          pay.map(id=>(id,(0, 0, 1)))
        }else{
          Nil
        }
      }
    }
    val rdd2: RDD[(String, (Int, Int, Int))] = rdd1.reduceByKey (
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )

    val sortedRDD = rdd2.sortBy {
      case (key, (value1, value2, value3)) =>
        (-value1, -value2, -value3) // 降序排列，使用负数表示降序
    }
    sortedRDD.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }


}
