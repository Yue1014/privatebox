
package edu.hku.cs.dft.examples

import org.apache.spark.sql.SparkSession

/**
  * Use a simple word count to test our api
  * Usage: DebuggingTest file
  */

object APIExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("API example")
      .config("spark.dft.tracking.mode", "full")
      .getOrCreate()

    val in = spark.sparkContext.parallelize(Seq(("Max", "X-Man", 3),
                                            ("Max", "Kingsman", 5),
                                            ("John", "Kingsman", 4)), 2)

    val movie_rating = in.taint(t => (1, 2, 3)).map(t => (t._2, t._3))

    val movie_count = movie_rating.reduceByKey(_ + _).zipWithTaint().collect().foreach(println)

    spark.stop()
  }
}
