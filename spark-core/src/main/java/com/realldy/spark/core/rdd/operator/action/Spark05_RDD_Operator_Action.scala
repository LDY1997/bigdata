package com.realldy.spark.core.rdd.operator.action

object Spark05_RDD_Operator_Action {

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
    //TODO save算子
    // saveAsSequenceFile要求数据必须数kv类型
    rdd.saveAsTextFile("output/Spark05_RDD_Operator_Action/saveAsTextFile")
    rdd.saveAsObjectFile("output/Spark05_RDD_Operator_Action/saveAsObjectFile")
    rdd.saveAsSequenceFile("output/Spark05_RDD_Operator_Action/saveAsSequenceFile")




    //TODO 关闭环境
    sc.stop()
  }


}
