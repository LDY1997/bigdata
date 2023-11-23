package com.realldy.spark.core.rdd.operator.transform

object Spark04_RDD_Operator_Transform2 {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(List(1,2),3,List(4,5)))
    val value = rdd.flatMap(
        _ match {
          case list:List[_] => list
          case dat:Int => List(dat)
        }

    )


    value.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
