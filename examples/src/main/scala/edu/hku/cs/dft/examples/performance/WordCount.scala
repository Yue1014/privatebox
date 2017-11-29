package edu.hku.cs.dft.examples.performance

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by max on 15/5/2017.
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    val input = args(0)

    val trace = args(1).toBoolean

    val partition = args(2).toInt

    val word = args(3)

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    var text = sc.textFile(input, partition)

    if (trace)
      text = text.zipWithUniqueId().taint(t => (t._2, -1)).map(_._1)

    val result =
      if (trace)
        text.map(t => (t, if (t.length > 0) (1 & (t.charAt(0) | 1)) else 1)).reduceByKey(_ + _)
      else
        text.map(t => (t, 1)).reduceByKey(_ + _)

    if (trace)
      result.zipWithTaint().saveAsObjectFile("word_count")
    else
      result.saveAsObjectFile("word_count")

    readLine()
    sc.stop()
  }
}
