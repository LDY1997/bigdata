package com.realldy.spark.core.rdd.operator.transform

object Spark19_RDD_Operator_Transform {

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
    //  三个参数
    //  第一参数：第一个数据进行转换
    //  第二个参数：分区内计算,类型需要明确指定
    //  第二个参数：分区间计算,类型需要明确指定

    val rdd1 = rdd.combineByKey(
      v=>(v,1),
      (t:(Int,Int),v)=>(t._1+v,t._2+1),
      (t1:(Int,Int), t2:(Int,Int)) =>(t1._1+t2._1,t1._2+t2._2)
    ).mapValues{
      case (i, i1) => {
        i/i1
      }
    }


    rdd1.collect().foreach(println)

    //TODO 关闭环境
    sc.stop()
  }


}
