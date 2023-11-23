package com.realldy.spark.core.rdd.operator.action

object Spark04_RDD_Operator_Action {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4),2)

    //TODO countByValue算子
    //  countByValue计算每个value出现的次数
    val countByValue: collection.Map[Int, Long] = rdd.countByValue()
    println(countByValue)

    //TODO countByKey算子
    //  countByKey计算每个key出现的次数
    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3)
    ))
    val countByKey: collection.Map[String, Long] = rdd1.countByKey()
    println(countByKey)


    //TODO 关闭环境
    sc.stop()
  }


}
