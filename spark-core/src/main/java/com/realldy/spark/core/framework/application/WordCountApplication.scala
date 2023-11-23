package com.realldy.spark.core.framework.application

import com.realldy.spark.core.framework.contorller.WordCountContorller
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApplication extends  App {

    val conf = new SparkConf().setMaster("local").setAppName("Wordcount").set("spark.testing.memory", "512000000")
    val sc = new SparkContext(conf)
    val contorller = new WordCountContorller
    contorller.wcc()
    sc.stop()


}
