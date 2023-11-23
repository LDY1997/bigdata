package com.realldy.spark.core.rdd.operator.transform

object Spark12_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(4,1,5,6,2,3,7,8),2)

//    //TODO 排序
    //  默认1为升序，不会改变分区，但是会shuffle
    val makeRdd1 = rdd.sortBy(x=>x,false)


    makeRdd1.saveAsTextFile("output/Spark12")

    //TODO 关闭环境
    sc.stop()
  }


}
