package com.realldy.spark.core.framework.dao
import com.realldy.spark.core.framework.application.WordCountApplication.sc

class WordCountDao {
  def readfile(path: String) = {
    //    读取文件
    sc.textFile(path)
  }
}
