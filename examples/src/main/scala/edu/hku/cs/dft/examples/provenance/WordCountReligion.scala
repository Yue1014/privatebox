package edu.hku.cs.dft.examples.provenance

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianyu on 5/1/17.
  */
object WordCountReligion {
  def main(args: Array[String]): Unit = {

    val intput = args(0)

    val trace = args(1).toBoolean

    val partition = args(2).toInt

    val isInt = args(3).toBoolean

    val conf = new SparkConf
    val sc = new SparkContext(conf)
    var text = sc.textFile(intput, partition)

    text = text.flatMap(_.split("\\s+"))

    val pair = text.map(t => (t, 1))

    val wc = pair.reduceByKey(_ + _).filter{case (w, c) =>
      w.equals("religion")
    }.count()

    println("religion wc:" + wc)

    readLine()
    sc.stop()
  }
}

