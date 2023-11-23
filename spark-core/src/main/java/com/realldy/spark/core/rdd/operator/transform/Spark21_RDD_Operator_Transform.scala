package com.realldy.spark.core.rdd.operator.transform

object Spark21_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3)
    ))
    val rdd1 = sc.makeRDD(List(("b", 4), ("e", 5), ("a", 6)
    ))

    //TODO join
    //  只会匹配两个数据源都有的数据
    //  如果两个数据源都有，会依次匹配，可能会出现笛卡尔乘积，数据量会集合增长
    val rdd2 = rdd.join(rdd1)


    rdd2.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
