package com.realldy.spark.core.framework.contorller

import com.realldy.spark.core.framework.service.WordCountService

class WordCountContorller {
  private val service = new WordCountService


  def wcc() ={

    //    对分组后的数据进行转换
    val arr = service.dataAnaliyze()
    arr.foreach(println)

  }

}
