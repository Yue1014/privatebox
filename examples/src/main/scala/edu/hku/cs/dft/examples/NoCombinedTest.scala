
package edu.hku.cs.dft.examples

import edu.columbia.cs.psl.phosphor.runtime.Taint
import edu.hku.cs.dft.tracker.CombinedTaint
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Use a simple word count to test our api
  * Usage: DebuggingTest file
  */

object NoCombinedTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    val file = args(0)

    val trace = args(1).toBoolean

    val same = args(2).toBoolean

    val text = sc.parallelize(1 to 1000000, 4)

    var n = text.map(t => {
      (t, t + 1)
    })

    if (trace && !same)
      n = n.zipWithUniqueId().taint{
        case ((left, right), id) =>
          (id, -1)
      }.map(_._1)

    if (trace && same) {
      val taint = new CombinedTaint(new Taint(1))
      n = n.zipWithUniqueId().taint {
        case ((left, right), id) =>
          (taint, -1)
      }.map(_._1)
    }

    n = n.reduceByKey(_ + _)

    n = n.map(t => (t._2 + t._1, t._2))
      .map(t => (t._1 + t._2, t._1))

    if (trace)
      n.collectWithTaint()
    else
      n.collect()
    readLine()
    sc.stop()
  }
}
