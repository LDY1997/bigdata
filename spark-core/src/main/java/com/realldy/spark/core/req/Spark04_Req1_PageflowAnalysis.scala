package com.realldy.spark.core.req

import org.apache.spark.rdd.RDD

object Spark04_Req1_PageflowAnalysis {

  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_create").set("spark.testing.memory", "512000000")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    //TODO 从内存创建RDD
    val rdd = sc.textFile("data/user_visit_action.txt")
    val mapRDD = rdd.map {
      line => {
        val data = line.split("_")
        UserVisitAction(
          data(0),
          data(1).toLong,
          data(2),
          data(3).toLong,
          data(4),
          data(5),
          data(6).toLong,
          data(7).toLong,
          data(8),
          data(9),
          data(10),
          data(11),
          data(12).toLong
        )
      }
    }

    //TODO 指定页面
    val ids: List[Long] = List(1, 2, 3, 4, 5, 6, 7)
    val okflowIds = ids.zip(ids.tail)

    //TODO 计算分母
    val fenmu: Map[Long, Int] = mapRDD
      .filter(
      t=>{
        ids.init.contains(t.page_id)
      }
    )
      .map(
      action => {
        (action.page_id, 1)
      }
    ).reduceByKey(_ + _).collect().toMap

    //TODO 计算分子
    val fenzi: RDD[((Long, Long), Int)] = mapRDD.groupBy(
        UserVisitAction => {
          UserVisitAction.session_id
        }
      )
      .mapValues(
        iter => {
          val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
          val page = sortList.map(_.page_id)
          val tuples: List[(Long, Long)] = page.zip(page.tail)
          tuples
        }
      ).map(_._2).flatMap(list => list)
      .filter(
        t=>{
          okflowIds.contains(t)
        }
      )
      .map {
        t => {
          ((t._1, t._2), 1)
        }
      }.reduceByKey(_ + _)

    //TODO 计算分子/分母
    fenzi.foreach {
      case ((v1, v2),sum) => {
        val int = fenmu.getOrElse(v1, 1).toFloat
        val sum1 = sum.toFloat
        println(s"${v1}装到${v2}为:"+sum1/int)
      }
    }
//    fenzi.collect().foreach(println)



    //TODO 关闭环境
    sc.stop()
  }


  //用户访问动作表
  case class UserVisitAction(
    date: String, //用户点击行为的日期
    user_id: Long, //用户的 ID
    session_id: String, //Session 的 ID
    page_id: Long, //某个页面的 ID
    action_time: String, //动作的时间点
    search_keyword: String, //用户搜索的关键词
    click_category_id: Long, //某一个商品品类的 ID
    click_product_id: Long, //某一个商品的 ID
    order_category_ids: String, //一次订单中所有品类的 ID 集合
    order_product_ids: String, //一次订单中所有商品的 ID 集合
    pay_category_ids: String, //一次支付中所有品类的 ID 集合
    pay_product_ids: String, //一次支付中所有商品的 ID 集合
    city_id: Long
  ) //城市 id


}
