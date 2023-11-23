package com.realldy.spark.core.framework.service

import com.realldy.spark.core.framework.dao.WordCountDao

class WordCountService {
  private val dao = new WordCountDao

  def dataAnaliyze() ={

    val line = dao.readfile("data/word.txt")

    //    将一行数据进行拆分
    val words = line.flatMap(_.split(" "))

    //单词计数
    val wordNum = words.map {
      (_, 1)
    }

    //统计
    val wordToCount = wordNum.reduceByKey(_ + _)
    wordToCount
  }

}
