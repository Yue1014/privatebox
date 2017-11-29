package edu.hku.cs.dft.examples.performance

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by max on 9/5/2017.
  */

// Sort in a group

object MedicalSort {

  def main(args: Array[String]): Unit = {

    val trace = if (args.length > 0 && args(0).equals("true")) {
      true
    } else
      false

    val session = new SparkConf()
    if (trace)
      session.set("spark.dft.tracking.mode", "full")

    val spark = new SparkContext(session)

    val file = if (args.length > 0) args(0) else throw new IllegalArgumentException()

    var records = spark.textFile(file, 8)
      .map(t => {
        val arr = t.split(",")
        //id clin illd temp country age cprot hit
        (arr(0), arr(3), arr(6).toInt, arr(11).toDouble, arr(14), arr(15).toInt, arr(16), arr(17))
      })
      .map{
        case (id, clinm, illd, temp, country, age, cprot, hit) => (id, country, temp)
      }

    if (trace)
      records = records.zipWithUniqueId().taint(m => {
        ((-1, m._2, -1), -1)
      }).map(_._1)

    val sortedRecord = records.map(t => {
      (t._2, t)
    })

    val result = sortedRecord.groupByKey().flatMapValues(t => {
      val g = t.toArray.sortBy(_._3)
      if (g.length < 10) {
        g
      } else {
        g.slice(0, 10)
      }
    })

    sortedRecord.mapValues(t => 1).reduceByKey(_ + _).collect().foreach(m => println(m._1 + " " + m._2))

    if (trace)
      result.zipWithTaint().saveAsObjectFile("medical_sort")
    else
      result.saveAsObjectFile("medical_sort")

    readLine()

    spark.stop()
  }
}
