package edu.hku.cs.dft.examples.performance

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianyu on 5/10/17.
  */

// Random sample of a group

object MedicalGroupBy {
  def main(args: Array[String]): Unit = {

    val trace = if (args.length > 1 && args(1).equals("true")) {
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

    if (trace)
      records = records.zipWithUniqueId().taint(m => {
        ((m._2, -1, -1), -1)
      }).map(_._1)

    val sortedRecord = records.map(t => {
      val sort =
        if (t._6 > 60)
          4
        else if (t._6 > 40)
          3
        else if (t._6 > 25)
          2
        else if (t._6 > 14)
          1
        else
          0
      (sort, t)
    })

    val result = sortedRecord.groupByKey().flatMapValues(t => {
      // age type, id
      var k: List[String] = List()
      val count = 0
      val limit = 50
      t.foreach(m => {
        if (count < limit) {
          k = m._1 :: k
        }
      })
      k
    })

    sortedRecord.mapValues(t => 1).reduceByKey(_ + _).collect().foreach(m => println(m._1 + " " + m._2))

    if (trace)
      result.zipWithTaint().saveAsObjectFile("medical_group")
    else
      result.saveAsObjectFile("medical_group")

    readLine()

    spark.stop()
  }
}
