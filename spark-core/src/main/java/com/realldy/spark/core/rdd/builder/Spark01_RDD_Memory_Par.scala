package com.realldy.spark.core.rdd.builder

object Spark01_RDD_Memory_Par {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
      .set("spark.default.parallelism","5")//设置五个并行度
     val sc = new SparkContext(sparkConf)

    //TODO 从内存创建RDD
    //     如果不设置分区数，先取spark.default.parallelism作为分区数，
    //     如果也没设置spark.default.parallelism，则取CPU的最大核数
    val rdd = sc.makeRDD(List(1,2,3,4))

    //TODO 保存文件
    rdd.saveAsTextFile("output")


    //TODO 关闭环境
    sc.stop()
  }


}
