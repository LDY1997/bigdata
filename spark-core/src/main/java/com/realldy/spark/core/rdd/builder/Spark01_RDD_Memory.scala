package com.realldy.spark.core.rdd.builder

object Spark01_RDD_Memory {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val seq = Seq[Int](1,2,3,4)
    //makeRDD底层调用parallelize，因此这两个方法是一样的
    //val rdd = sc.parallelize(seq)
    val rdd = sc.makeRDD(seq)

    rdd.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
