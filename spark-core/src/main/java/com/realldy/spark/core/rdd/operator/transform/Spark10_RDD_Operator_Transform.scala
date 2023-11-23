package com.realldy.spark.core.rdd.operator.transform

object Spark10_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),3)

//    //TODO 转换函数
    //  默认不会打乱重组合，可能会导致数据倾斜
    // 可以用shuffle处理，即第二个参数改为true
    val makeRdd1 = rdd.coalesce(2,true)

    makeRdd1.saveAsTextFile("output/Spark10-01")

    //TODO 关闭环境
    sc.stop()
  }


}
