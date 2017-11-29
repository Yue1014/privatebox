package edu.hku.cs.dft.examples.provenance

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by max on 15/5/2017.
  */
object WordGrepCount {
  def main(args: Array[String]): Unit = {

    val input = args(0)

    val trace = args(1).toBoolean

    val partition = args(2).toInt

    val word = args(3)

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    var text = sc.textFile(input, partition)

    val wc = text.filter(t => t.contains(word)).count()

    println("science: " + wc)
    readLine()
    sc.stop()
  }
}
