package edu.hku.cs.dft.examples.performance

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianyu on 5/10/17.
  */
object TentativeClosure {

  def main(args: Array[String]) {
    val spark = new SparkContext(new SparkConf())

    val file = args(0)

    val partition = args(2).toInt

    val trace = args.length > 1 && args(1).equals("true")

    var tc = spark.textFile(file, partition)
      .map(t => {
        val splits = t.split("\\s+")
        (splits(0), splits(1))
      })

    if (trace)
      tc = tc.zipWithUniqueId()
      .taint(t => {
        ((t._2, t._2), -1)
      })
      .map(_._1)
    // Linear transitive closure: each round grows paths by one edge,
    // by joining the graph's edges with the already-discovered paths.
    // e.g. join the path (y, z) from the TC with the edge (x, y) from
    // the graph to obtain the path (x, z).

    // Because join() joins on keys, the edges are stored in reversed order.
    val edges = tc.map(x => (x._2, x._1))

//    tc.map(m => (m._1, 1)).reduceByKey(_ + _).collect().foreach(println)
    var total_run = 0

    // This join is iterated until a fixed point is reached.
    do {
      // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
      // then project the result to obtain the new (x, z) paths.
      tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct(partition)
      total_run += 1
    } while (total_run < 1)

    if (trace)
      tc.zipWithTaint().saveAsObjectFile("sparktc")
    else
      tc.saveAsObjectFile("sparktc")
    readLine()
//    println("TC has " + tc.count() + " edges.")
    spark.stop()
  }
}
