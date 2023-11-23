package com.realldy.spark.core.rdd.operator.transform

object Spark08_RDD_Operator_Transform {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))

//    //TODO 转换函数

//  sample有三个参数，（是否返回，每条数据抽取的概率（不放回）/抽取的次数（放回），随机种子）

    val makeRdd1 = rdd.sample(false,0.4,1)

    makeRdd1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
