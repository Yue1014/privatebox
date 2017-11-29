package edu.hku.cs.dft.examples.provenance

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import java.io.IOException
import java.lang.{Integer => JavaInteger}

import scala.collection.mutable

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
/**
  * Created by jianyu on 5/10/17.
  */
object ALSPrediction {
  def main(args: Array[String]): Unit = {
    val path = if (args.length > 0)
      args(0)
    else
      throw new IllegalArgumentException("no path")

    val trace = args.length > 1 && args(1).equals("true")

    val session = new SparkConf()
    if (trace)
      session.set("spark.dft.tracking.mode", "full")

    val spark = new SparkContext(session)

    val testData = spark.objectFile("als/test").asInstanceOf[RDD[Rating]].map(x => (x.user, x.product))

    val userFeatures = spark.objectFile("als/userf")

    val productFeatures = spark.objectFile("als/prof")

    val result = predict(testData, userFeatures.asInstanceOf[RDD[(Int, Array[Double])]],
      productFeatures.asInstanceOf[RDD[(Int, Array[Double])]])

    if (trace)
      result.collectWithTaint()
    else
      result.collect()

    readLine()
    spark.stop()
  }

  private[this] def countApproxDistinctUserProduct(usersProducts: RDD[(Int, Int)]): (Long, Long) = {
    val zeroCounterUser = new HyperLogLogPlus(4, 0)
    val zeroCounterProduct = new HyperLogLogPlus(4, 0)
    val aggregated = usersProducts.aggregate((zeroCounterUser, zeroCounterProduct))(
      (hllTuple: (HyperLogLogPlus, HyperLogLogPlus), v: (Int, Int)) => {
        hllTuple._1.offer(v._1)
        hllTuple._2.offer(v._2)
        hllTuple
      },
      (h1: (HyperLogLogPlus, HyperLogLogPlus), h2: (HyperLogLogPlus, HyperLogLogPlus)) => {
        h1._1.addAll(h2._1)
        h1._2.addAll(h2._2)
        h1
      })
    (aggregated._1.cardinality(), aggregated._2.cardinality())
  }

  def predict(usersProducts: RDD[(Int, Int)], userFeatures: RDD[(Int, Array[Double])], productFeatures: RDD[(Int, Array[Double])]): RDD[Rating] = {
    // Previously the partitions of ratings are only based on the given products.
    // So if the usersProducts given for prediction contains only few products or
    // even one product, the generated ratings will be pushed into few or single partition
    // and can't use high parallelism.
    // Here we calculate approximate numbers of users and products. Then we decide the
    // partitions should be based on users or products.
    val (usersCount, productsCount) = countApproxDistinctUserProduct(usersProducts)

    if (usersCount < productsCount) {
      val users = userFeatures.join(usersProducts).map(t => {
        (t._2._2, (t._1, t._2._1))
      })
      users.join(productFeatures).map(t =>
        Rating(t._2._1._1, t._1, blas.ddot(t._2._1._2.length, t._2._1._2, 1, t._2._2, 1))
      )
    } else {
      val products = productFeatures.join(usersProducts.map(_.swap)).map {
        case (product, (pFeatures, user)) => (user, (product, pFeatures))
      }
      products.join(userFeatures).map {
        case (user, ((product, pFeatures), uFeatures)) =>
          Rating(user, product, blas.ddot(uFeatures.length, uFeatures, 1, pFeatures, 1))
      }
    }
  }

}
