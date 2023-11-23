package com.realldy.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark02_SqarkSQL_UDF {

  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL").set("spark.testing.memory", "512000000")
    val sc = SparkSession.builder().config(sparkConf).getOrCreate()
    import sc.implicits._

    //TODO 执行逻辑操作
    val df = sc.read.json("data/user.json")
    df.createOrReplaceTempView("user")

    sc.udf.register("prefixName",(name:String)=>{
      "Name"+name
    })

    sc.sql("select age,prefixName(username) from user").show()

    //TODO 关闭环境
    sc.stop()
  }
  case class User(id:Int,name:String,age:Int)


}
