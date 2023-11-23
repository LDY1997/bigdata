package com.realldy.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_SqarkSQL_Basic {

  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL").set("spark.testing.memory", "512000000")
    val sc = SparkSession.builder().config(sparkConf).getOrCreate()
    import sc.implicits._

    //TODO 执行逻辑操作
    //DataFrame
//    val df = sc.read.json("data/user.json")
//    df.show()
    //DataFrame sql
//    df.createTempView("user")
//    sc.sql("select * from user").show()
//    sc.sql("select avg(age) from user").show()


    //TODO DataFrame dql
    //在使用DataFrame是，如果涉及转换操作，需要引入转换规则
//    df.select("age","username").show()
//    df.select($"age"+1).show()

    //TODO DataSet
    //DataFrame是一种特殊的DataSet
//    val seq = Seq(1, 2, 3, 4)
//    val ds = seq.toDS()
//    ds.show()

    //RDD <=> DataFrame
    val value = sc.sparkContext.makeRDD(List((1, "zhangsan", 22), (2, "lisi", 23), (3, "wangwu", 25)))
    var df1 = value.toDF("id","name","age")
    val rdd = df1.rdd
//    df1.show()

    //DataFrame <=> DataSet
    val ds1 = df1.as[User]
//    ds1.show()
    df1 = ds1.toDF()

    //RDD <=> DataSet
    val ds2 = value.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    ds2.show()

    //TODO 关闭环境
    sc.stop()
  }
  case class User(id:Int,name:String,age:Int)


}
