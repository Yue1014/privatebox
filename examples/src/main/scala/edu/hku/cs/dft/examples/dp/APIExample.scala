
package edu.hku.cs.dft.examples.dp

import edu.hku.cs.dft.dp.{DPAggregator, NaiveDPModel}
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Use a simple word count to test our api
  * Usage: DebuggingTest file
  */

object APIExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("API example")
      .getOrCreate()

    val in = spark.sparkContext.parallelize(Seq(("Max", "X-Man", 3),
                                            ("Max", "Kingsman", 5),
                                            ("John", "Kingsman", 4)), 2)
    val result = in.map(t => (Random.nextDouble(), t)).sortBy(_._1).map(_._2).zipWithTaint().collectSplit(2)
    DPAggregator.dp_aggregate(result, new NaiveDPModel(3)).foreach(println)
    spark.stop()
  }
}
