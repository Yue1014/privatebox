package edu.hku.cs.dft.examples.debug

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianyu on 4/13/17.
  */
object FlowErrorExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("flow error example")
      .set("spark.dft.tracking.mode", "debug")
    val sc = new SparkContext(conf)

    val n = 2

    val array_a = sc.parallelize(Array((1, 1, 1),
      (1, 2, 2), (2, 1, 5), (2, 2, 8)))

    val array_b = sc.parallelize(Array((1, 1, 4),
      (1, 2, 3), (2, 1, 9), (2, 2, 2)))

    val mapOutA = array_a.flatMap(t => {
      for {i <- 0 to n}
        yield ((t._1, i, t._2), t._3)
    })

    val mapOutputB = array_b.flatMap(t => {
      for {i <- 0 to n}
        yield ((i, t._2, t._1), t._3)
    })

    val mapOutput = mapOutA.union(mapOutputB)

    mapOutput.reduceByKey(_ * _)
      .map(t => ((t._1._1, t._1._2), t._2))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println)

    sc.stop()

  }

}
