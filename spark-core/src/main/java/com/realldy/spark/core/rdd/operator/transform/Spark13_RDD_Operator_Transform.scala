package com.realldy.spark.core.rdd.operator.transform

object Spark13_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd1 = sc.makeRDD(List(1,2,3,4))
    val rdd2 = sc.makeRDD(List(3,4,5,6))


    //TODO 交集，数据类型一致
    // 3,4
    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))

    //TODO 并集，数据类型一致
    //1,2,3,4,3,4,5,6
    val rdd4 = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    //TODO 差集，数据类型一致
    //1,2
    val rdd5 = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    //TODO 拉链,要求分区数一致且长度一致
    //(1,3),(2,4),(3,5),(4,6)
    val rdd6 = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    //TODO 关闭环境
    sc.stop()
  }


}
