package com.realldy.spark.core.rdd.operator.transform

import java.text.SimpleDateFormat

object Spark07_RDD_Operator_Transform_Tets {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.textFile("data/apache.log")

    val rdd1 = rdd.filter(
        line => {
          val time = line.split(" ")(3)
         time.startsWith("17/05/2015")
        }
    )

    rdd1.collect().foreach(println)
    //TODO 关闭环境
    sc.stop()
  }


}
