package edu.hku.cs.dft.examples.provenance

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianyu on 5/3/17.
  */
object TwitterForward {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val twitterfile = args(0)

    val sc = new SparkContext(conf)

    var file = sc.textFile(twitterfile, 10)

    val to = file.map(
      line => {
        val list = line.split(",")
        if(list.size > 4){
          val time = list(0).split(" ")(3).substring(0, 7)
          val text = list(3)
          (time, text)
        }
        else{
          ("15:02:45","  ")
        }
      }

    )

    val file1 = to.groupByKey()

    val top_words = file1.map(t => {
      val lines = t._2
      val c = lines.count(_.contains("#FAKENEWS"))
      if (c > 0) {
        lines.flatMap(_.split("\\s+")).groupBy(t => t).keys.size
      }
      else
        0
    })

    val su = top_words.collect().sum

    println("c " + su)

    readLine()

    sc.stop()
  }
}
