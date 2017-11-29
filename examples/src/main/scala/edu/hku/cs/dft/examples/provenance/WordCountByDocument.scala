package edu.hku.cs.dft.examples.provenance

import org.apache.spark.sql.SparkSession

/**
  * Created by jianyu on 5/10/17.
  */
object WordCountByDocument {
  def main(args: Array[String]): Unit = {

    val trace = if (args.length > 0 && args(0).equals("true")) {
      true
    } else
      false

    val session = SparkSession
      .builder()
      .appName("WordCount Document")
    if (trace)
      session.config("spark.dft.tracking.mode", "full")

    val spark = session.getOrCreate()

    var records = spark.sparkContext.parallelize(Seq(
      ("A", 20, "cancer", "Da"),
      ("B", 45, "brain cancer", "Da"),
      ("C", 25, "heart disease", "Dc"),
      ("D", 50, "klsdf", "Dd")), 4)

    if (trace)
      records = records.zipWithUniqueId().taint(m => {
        ((m._2, -1, -1), -1)
      }).map(_._1)

    val sortedRecord = records.map(t => {
      val sort =
        if (t._2 > 60)
          3
        else if (t._2 > 40)
          2
        else if (t._2 > 15)
          1
        else
          0
      (sort, t)
    })

    val result = sortedRecord.groupByKey().flatMapValues(t => {
      var k: List[(String, Int, String, String)] = List()
      val count = 0
      val limit = 1
      t.foreach(m => {
        if (count < limit) {
          k = m :: k
        }
      })
      k
    })

    if (trace)
      result.collectWithTaint().foreach(println)
    else
      result.collect().foreach(println)

    readLine()

    spark.stop()
  }
}
