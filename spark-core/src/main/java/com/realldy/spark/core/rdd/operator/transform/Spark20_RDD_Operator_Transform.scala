package com.realldy.spark.core.rdd.operator.transform

object Spark20_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)), 2)


    //TODO combineByKey
    //  reduceByKey,aggregateByKey,foldByKey都是调用combineByKey方法
    val rdd1 = rdd.combineByKey(
      v => (v),
      (t: Int, v) => (t + v),
      (t1: Int, t2: Int) => (t1 + t2)
    )
    val rdd2 = rdd.reduceByKey(_ + _)
    val rdd3 = rdd.aggregateByKey(0)(_ + _, _ + _)
    val rdd4 = rdd.foldByKey(0)(_ + _)



    rdd1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
