package com.realldy.spark.core.rdd.operator.transform

object Spark03_RDD_Operator_Transform1 {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val value = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map((index,_))
      }
    )


    value.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
