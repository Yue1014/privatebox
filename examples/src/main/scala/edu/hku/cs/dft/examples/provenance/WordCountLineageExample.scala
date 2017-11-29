package edu.hku.cs.dft.examples.provenance

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianyu on 5/1/17.
  */
object WordCountLineageExample {
  def main(args: Array[String]): Unit = {

    val intput = args(0)

    val trace = args(1).toBoolean

    val partition = args(2).toInt

    val isInt = args(3).toBoolean

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    var text = sc.textFile(intput, partition)

    if (trace)
      text = text.zipWithUniqueId().taint(t => {
        if (isInt)
          (1, -1)
        else
          (t._2, -1)
    })
      .map(_._1)

    val wc = text.flatMap(t => t.split("\\s+"))
      .map(t => (t, if (t.length > 0) (t.charAt(0) | 1) & 1 else 1))
      .reduceByKey(_ + _)

    if (trace)
      wc.zipWithTaint().saveAsObjectFile("word_out")
    else
      wc.saveAsObjectFile("word_out")
    readLine()
    sc.stop()
  }
}

