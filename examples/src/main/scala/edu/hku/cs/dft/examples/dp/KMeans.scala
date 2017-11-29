/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package edu.hku.cs.dft.examples.dp

import breeze.linalg.{DenseVector, Vector, squaredDistance}
import edu.hku.cs.dft.dp._
import edu.hku.cs.dft.examples.dp.KMeans.closestPoint
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * K-means clustering.
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.ml.clustering.KMeans.
 */
object KMeans {

  def parseVector(line: String): (Vector[Double], Int) = {
    val sp = line.split(' ')
    val newline = sp.drop(0)
    val level = sp.head.toInt
    (DenseVector(newline.map(_.toDouble)), level)
  }

  def trueValue(spark: SparkContext, args: Array[String]): Array[scala.Vector[Double]] = {
    val partitions = args(3).toInt

    val splits = args(4).toInt
    val lines = spark
      .textFile(args(0), partitions)

    val data = lines.map(parseVector _)
    val K = args(1).toInt
    val convergeDist = args(2).toDouble

    val kPoints = data.takeSample(withReplacement = false, K, 42)
    var tempDist = 1.0

    while (tempDist > convergeDist) {
      val closest = data.map(p => {
        val k = closestPoint(p, kPoints)
        (k._1, (p._1, 1, k._2))
      })

      val pointStats = closest.reduceByKey { case ((p1, c1, l1), (p2, c2, l2)) => (p1 + p2, c1 + c2, l1 | l2) }

      val newPoints = pointStats.map { pair =>
        (pair._1, (pair._2._1 * (1.0 / pair._2._2), pair._2._3))
      }.collectAsMap()

      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i)._1, newPoints(i)._1)
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }
    }
    kPoints.map(t => t._1.activeValuesIterator.toVector)
  }

  def closestPoint(p: (Vector[Double], Int), centers: Array[(Vector[Double], Int)]): (Int, Int) = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p._1, centers(i)._1)
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    (bestIndex, centers(bestIndex)._2 | p._2)
  }

  def closestDistance(p: Vector[Double], centers: Array[Vector[Double]]): Double = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    closest
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkKMeans <file> <k> <convergeDist> <partitions> <split>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("SparkKMeans")
      .getOrCreate()

    val partitions = args(3).toInt

    val splits = args(4).toInt

    val lines = spark.sparkContext
      .textFile(args(0), partitions)
      .randomSplit(new Array[Double](splits)
      .map(t => 1 / splits.toDouble))
    var result: scala.Vector[Array[(scala.Vector[Double], Any)]] = scala.Vector()
    for (j <- 1 to splits) {
      val data = lines(0).map(parseVector _)
      val K = args(1).toInt
      val convergeDist = args(2).toDouble

      val kPoints = data.takeSample(withReplacement = false, K, 42)
      var tempDist = 1.0

      while (tempDist > convergeDist) {
        val closest = data.map(p => {
          val k = closestPoint(p, kPoints)
          (k._1, (p._1, 1, k._2))
        })

        val pointStats = closest.reduceByKey { case ((p1, c1, l1), (p2, c2, l2)) => (p1 + p2, c1 + c2, l1 | l2) }

        val newPoints = pointStats.map { pair =>
          (pair._1, (pair._2._1 * (1.0 / pair._2._2), pair._2._3))
        }.collectAsMap()

        tempDist = 0.0
        for (i <- 0 until K) {
          tempDist += squaredDistance(kPoints(i)._1, newPoints(i)._1)
        }

        for (newP <- newPoints) {
          kPoints(newP._1) = newP._2
        }
        println("Finished iteration (delta = " + tempDist + ")")
      }
      val point_taint = kPoints.map(t => {
        val length = t._1.length
        (t._1.activeValuesIterator.toVector, new Array[Int](length).toVector.map(g => t._2).asInstanceOf[Any])
      })
      println("Final centers:")
      result ++= scala.Vector(point_taint)
    }

    val tv = trueValue(spark.sparkContext, args)

    val none_r = DPAggregator.dp_aggregate(result, new NoneDPModel)
    val delta = DPAggregator.delta(result)

    val gupt_r = DPAggregator.dp_aggregate(result, new GUPTDPModel(splits))
    println("gupt:: " + DPAggregator.noise_magnitude(gupt_r, none_r, delta).asInstanceOf[scala.Vector[Double]].sum / 75)
    println("gupt:: " + DPAggregator.noise_accuracy(gupt_r, tv, delta).asInstanceOf[scala.Vector[Double]].sum / 75)

    val naive_r = DPAggregator.dp_aggregate(result, new NaiveDPModel(splits))
    println("naive:: " + DPAggregator.noise_magnitude(naive_r, none_r, delta).asInstanceOf[scala.Vector[Double]].sum / 75)
    println("naive:: " + DPAggregator.noise_accuracy(naive_r, tv, delta).asInstanceOf[scala.Vector[Double]].sum / 75)

    val combine_r = DPAggregator.dp_aggregate(result, new CombinedDPModel)
    println("combine:: " + DPAggregator.noise_magnitude(combine_r, none_r, delta).asInstanceOf[scala.Vector[Double]].sum / 75)
    println("combine:: " + DPAggregator.noise_accuracy(combine_r, tv, delta).asInstanceOf[scala.Vector[Double]].sum / 75)

    val split_r = DPAggregator.dp_aggregate(result, new SplitDPModel(splits))
    println("split:: " + DPAggregator.noise_magnitude(split_r, none_r, delta).asInstanceOf[scala.Vector[Double]].sum / 75)
    println("split:: " + DPAggregator.noise_accuracy(split_r, tv, delta).asInstanceOf[scala.Vector[Double]].sum / 75)

    val kPoints_gupt = gupt_r.map(t => {
      val a = t.asInstanceOf[Iterable[Double]]
      Vector(a.toArray)
    }).toArray
    val gupt_var = spark.sparkContext
      .textFile(args(0), partitions)
      .map(parseVector _)
      .map(_._1)
      .map(t => closestDistance(t, kPoints_gupt))
      .sum()

    val kPoints_combine = combine_r.map(t => {
      val a = t.asInstanceOf[Iterable[Double]]
      Vector(a.toArray)
    }).toArray
    val com_var = spark.sparkContext
      .textFile(args(0), partitions)
      .map(parseVector _)
      .map(_._1)
      .map(t => closestDistance(t, kPoints_combine))
      .sum()

    val kPoints_split = split_r.map(t => {
      val a = t.asInstanceOf[Iterable[Double]]
      Vector(a.toArray)
    }).toArray
    val split_var = spark.sparkContext
      .textFile(args(0), partitions)
      .map(parseVector _)
      .map(_._1)
      .map(t => closestDistance(t, kPoints_split))
      .sum()

    println("variance gupt, com, split: " + gupt_var + " " + com_var + " " + split_var)

    spark.stop()
  }
}
// scalastyle:on println
