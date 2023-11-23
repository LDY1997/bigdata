package com.realldy.spark.core.rdd.operator.transform

object Spark01_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4))

//    //TODO 转换函数
//    def mapFunction(num:Int): Int = {
//      num * 2
//    }
//
//    val makeRdd = rdd.map(mapFunction)

    val makeRdd1 = rdd.map(_ * 2)

    makeRdd1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
