
package edu.hku.cs.dft.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Use a simple word count to test our api
  * Usage: DebuggingTest file
  */

object SpeedTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val number = args(0).toInt
    val in = sc.parallelize(1 to number, 4)
    val process = in.map(t => {
      (t, t * (t - 1))
    }).taint(t => (1, 2))

    val mod = process.map(t => {
      (t._1 % 100, t._2)
    })

    mod.reduceByKey((x, y) => x + y).zipWithTaint().collect().foreach(println)

    readLine()

    sc.stop()

  }
}
