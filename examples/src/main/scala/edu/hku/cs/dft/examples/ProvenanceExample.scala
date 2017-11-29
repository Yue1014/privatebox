package edu.hku.cs.dft.examples

import org.apache.spark.sql.SparkSession

/**
  * Created by max on 21/4/2017.
  */
object ProvenanceExample {
  case class Tracer(objId: Int, key: Int)

  var counter: Int = 0

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Provenance Example")
      .config("spark.dft.tracking.mode", "full")
      .getOrCreate()

    val file = if (args.length > 0)
      args(0)
    else
      throw new Exception("please give input filename")

    val text = spark.read.textFile(file).rdd
    val lines = text.flatMap(t => t.split("\\s+"))
    val words = lines.map(word => (word, 1))
    val taintedWords = words.taint(t => {
      counter += 1
      val c = counter
      (Tracer(c, 1), Tracer(c, 2))
    })

    val word_count = taintedWords.reduceByKey(_ + _)
    val result_taint = word_count.collectWithTaint()
    result_taint.foreach(r => {
      println(r._1 + ":" + r._2)
    })

    spark.stop()
  }
}