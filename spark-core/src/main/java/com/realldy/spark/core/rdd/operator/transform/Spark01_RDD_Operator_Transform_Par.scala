package com.realldy.spark.core.rdd.operator.transform

object Spark01_RDD_Operator_Transform_Par {

  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
    val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //note rdd按分区逐个执行，不同分区并行处理，因此分区间处理数据是有序的，分区外处理数据是无序的
    val makeRdd1 = rdd.map(
      num => {
        println(num)
        num
      }
    )
    val makeRdd2 = makeRdd1.map(
      num => {
        println(num)
        num
      }
    )

    makeRdd2.collect()

    //TODO 关闭环境
    sc.stop()
  }


}
