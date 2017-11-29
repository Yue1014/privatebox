package edu.hku.cs.dft

import edu.hku.cs.dft.dp._

object NaiveDPAggregatorSuit {
  def main(args: Array[String]): Unit = {
    val testInput: Vector[Array[((Int, Int), Any)]] = Vector(
      Array(((1, 2), (0, 0)), ((1, 2), (0, 0)), ((1, 3), (0, 0))),
      Array(((1, 4), (0, 0)), ((2, 1), (0, 0)), ((3, 4), (0, 0))))
    DPAggregator.dp_aggregate(testInput, new NaiveDPModel(2)).foreach(println)

    val testInput2: Vector[Array[(Int, Any)]] = Vector(
      Array((1, 0), (2, 0), (3, 0)),
      Array((2, 0), (1, 0), (0, 0)))
    DPAggregator.dp_aggregate(testInput2, new NaiveDPModel(2)).foreach(println)

    val testInput3: Vector[Array[((Double, Double, String, Int, Int), Any)]] = Vector(
      Array(((1, 2, "1", 1, 1), (0, 1, 0, 0, 0)), ((1, 2, "1", 1, 1), (0, 0, 0, 0, 0)), ((1, 3, "1", 1, 1), (0, 0, 0, 0, 0))),
      Array(((1, 4, "1", 1, 1), (0, 0, 0, 0, 0)), ((2, 1, "1", 1, 1), (0, 0, 0, 0, 0)), ((3, 4, "1", 1, 2), (0, 0, 0, 0, 0))))
//    val d = DPAggregator.dp_aggregate(testInput3, new CombinedDPModel)
//    DPAggregator.noise_magnitude(d, DPAggregator.dp_aggregate(testInput3, new NoneDPModel), DPAggregator.delta(testInput3))
//    DPAggregator.noise_magnitude(DPAggregator.dp_aggregate(testInput3, new SplitDPModel(2)), DPAggregator.dp_aggregate(testInput3, new NoneDPModel), DPAggregator.delta(testInput3))
    println(DPAggregator.noise_magnitude(DPAggregator.dp_aggregate(testInput3, new SplitDPModel(2)), DPAggregator.dp_aggregate(testInput3, new CombinedDPModel), DPAggregator.delta(testInput3)))
    println(DPAggregator.noise_accuracy(DPAggregator.dp_aggregate(testInput3, new SplitDPModel(2)), DPAggregator.dp_aggregate(testInput3, new CombinedDPModel), DPAggregator.delta(testInput3)))
  }
}
