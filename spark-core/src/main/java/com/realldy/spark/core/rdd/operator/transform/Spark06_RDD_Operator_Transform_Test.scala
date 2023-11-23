package com.realldy.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

object Spark06_RDD_Operator_Transform_Test {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.textFile("data/apache.log")

    val rdd1 = rdd.map(
        line => {
          val time = line.split(" ")(3)
          val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
          val date = sdf.parse(time)
          val sdf1 = new SimpleDateFormat("HH")
          val hour = sdf1.format(date)
          (hour, 1)
        }
      )
      .groupBy(_._1)
      .map {
        case (hour, iter) => {
          (hour, iter.size)
        }
      }

    rdd1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
