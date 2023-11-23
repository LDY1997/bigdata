package com.realldy.spark.core.rdd.builder

object Spark01_RDD_Memory_Par1 {

  import org.apache.spark.{SparkConf, SparkContext}
  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
      .set("spark.default.parallelism","5")//设置五个并行度
     val sc = new SparkContext(sparkConf)

    //TODO 从内存创建RDD
    //     start=(i*length)/numSlices 闭
    //     end=((i+1)*length)/numSlices 开

    //    val rdd = sc.makeRDD(List(1,2,3,4),2)//[1,2],[3,4]

    val rdd = sc.makeRDD(List(1,2,3,4),3)//[0，1),[1,2),[2,4)         【1】，【2】,【3，4】


    //TODO 保存文件
    rdd.saveAsTextFile("output/par1_2")


    //TODO 关闭环境
    sc.stop()
  }


}
