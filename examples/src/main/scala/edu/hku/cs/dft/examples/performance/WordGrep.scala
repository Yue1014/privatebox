package edu.hku.cs.dft.examples.performance

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by max on 15/5/2017.
  */
object WordGrep {
  def main(args: Array[String]): Unit = {

    val input = args(0)

    val trace = args(1).toBoolean

    val partition = args(2).toInt

    val isInt = args(3).toBoolean

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    var text = sc.textFile(input, partition)

    if (trace)
      text = text.zipWithUniqueId().taint(t => {
        if (isInt)
          (1, -1)
        else
          (t._2, -1)
      })
        .map(_._1)

    val wc = text.filter(t => t.contains("science"))

    if (trace)
      wc.zipWithTaint().saveAsObjectFile("grep_out")
    else
      wc.saveAsObjectFile("grep_out")
    readLine()
    sc.stop()
  }
}
