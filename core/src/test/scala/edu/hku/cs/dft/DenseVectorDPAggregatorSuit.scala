package edu.hku.cs.dft

import breeze.linalg.DenseVector
import edu.hku.cs.dft.dp.{DPAggregator, NaiveDPModel}

object DenseVectorDPAggregatorSuit {
  def main(args: Array[String]): Unit = {
    val testInput: Vector[scala.Array[(scala.Vector[Double], Any)]] = scala.Vector(Array(
      (DenseVector(.3, .2, .1).activeValuesIterator.toVector, new Array[Int](3).toVector.map(t => 1).asInstanceOf[Any]),
        (DenseVector(.1, .2, .3).activeValuesIterator.toVector, new Array[Int](3).toVector.map(t => 1).asInstanceOf[Any])
    ))
    DPAggregator.dp_aggregate(testInput, new NaiveDPModel(1)).foreach(println)

  }
}
