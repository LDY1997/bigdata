package com.realldy.spark.core.rdd.operator.transform

object Spark11_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)

//    //TODO 扩大分区
    //  如果要扩大分区，用coalesce需要进行shuffle
//    val makeRdd1 = rdd.coalesce(3,true)

    //扩大分区建议使用repartition
    val makeRdd1 = rdd.repartition(3)


    makeRdd1.saveAsTextFile("output/Spark11")

    //TODO 关闭环境
    sc.stop()
  }


}
