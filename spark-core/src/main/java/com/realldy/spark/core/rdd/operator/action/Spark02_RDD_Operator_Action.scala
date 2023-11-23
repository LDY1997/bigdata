package com.realldy.spark.core.rdd.operator.action

object Spark02_RDD_Operator_Action {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
     val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.makeRDD(List(1,2,3,4))

    //TODO reduce算子
    //  直接返回数据类型，不再是rdd
    val i = rdd.reduce(_ + _)
    println(i)

    //TODO reduce算子
    //  将不同分区的数据按照分区的顺序返回到drive端
    val ints: Array[Int] = rdd.collect()
    println(ints.mkString(","))

    //TODO count算子
    //  数据源数据个数
    val count: Long = rdd.count()
    println(count)


    //TODO first算子
    //  数据源第一个数据
    val first: Int = rdd.first()
    println(first)

    //TODO take
    //  数据源前n个数据
    val ints1: Array[Int] = rdd.take(3)
    println(ints1.mkString(","))

    //TODO takeOrdered
    //  排序后，取前n个数据
    val rdd1 = sc.makeRDD(List(4,2,3,1))
    val ints2: Array[Int] = rdd1.takeOrdered(3)
    println(ints2.mkString(","))

    //TODO 关闭环境
    sc.stop()
  }


}
