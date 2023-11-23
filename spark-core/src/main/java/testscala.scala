object testscala {

  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Wordcount").set("spark.testing.memory", "512000000")

    val sc = new SparkContext(conf)

    //    读取文件
    val line = sc.textFile("word.txt")

    //    将一行数据进行拆分
    val words = line.flatMap(_.split(" "))

    //    将单词进行分组
    val groupWord = words.groupBy(t=>t)

    //    数据根据单词进行分组，便于统计
    val wordToCount = groupWord.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //    对分组后的数据进行转换
    val arr = wordToCount.collect()
    arr.foreach(println)


    sc.stop()
  }


}
