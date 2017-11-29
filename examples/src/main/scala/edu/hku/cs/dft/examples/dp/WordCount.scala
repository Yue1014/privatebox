package edu.hku.cs.dft.examples.dp

import edu.hku.cs.dft.dp._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by max on 15/5/2017.
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    val input = args(0)

    val partition = args(1).toInt

    val splits = args(2).toInt

    val conf = new SparkConf()
    conf.set("spark.dft.tracking.mode", "full")

    val sc = new SparkContext(conf)
    var text = sc.textFile(input, partition)

    text = text.taint(t => {
      val i = Random.nextInt(100)
      if (i > 98)
        8
      else if (i > 94)
        4
      else if (i > 79)
        2
      else if (i > 49)
        1
      else
        0
    })

    val result = text.map(t => (t, if (t.length > 0) (1 & (t.charAt(0) | 1)) else 1)).reduceByKey(_ + _)

    val splitResult = result.zipWithTaint().collectSplit(splits)

    val tv = result.collect().sortBy(_._1)

    val none_r = DPAggregator.dp_aggregate(splitResult, new NoneDPModel)
    val delta = DPAggregator.delta(splitResult)

    val gupt_r = DPAggregator.dp_aggregate(splitResult, new GUPTDPModel(splits))
    println("gupt:: " + DPAggregator.noise_magnitude(gupt_r, none_r, delta).asInstanceOf[(String, Double)]._2)
    println("gupt:: " + DPAggregator.noise_accuracy(gupt_r, tv, delta).asInstanceOf[(String, Double)]._2)

    val naive_r = DPAggregator.dp_aggregate(splitResult, new NaiveDPModel(splits))
    println("naive:: " + DPAggregator.noise_magnitude(naive_r, none_r, delta).asInstanceOf[(String, Double)]._2)
    println("naive:: " + DPAggregator.noise_accuracy(naive_r, tv, delta).asInstanceOf[(String, Double)]._2)

    val combine_r = DPAggregator.dp_aggregate(splitResult, new CombinedDPModel)
    println("combine:: " + DPAggregator.noise_magnitude(combine_r, none_r, delta).asInstanceOf[(String, Double)]._2)
    println("combine:: " + DPAggregator.noise_accuracy(combine_r, tv, delta).asInstanceOf[(String, Double)]._2)

    val split_r = DPAggregator.dp_aggregate(splitResult, new SplitDPModel(splits))
    println("split:: " + DPAggregator.noise_magnitude(split_r, none_r, delta).asInstanceOf[(String, Double)]._2)
    println("split:: " + DPAggregator.noise_accuracy(split_r, tv, delta).asInstanceOf[(String, Double)]._2)

    readLine()
    sc.stop()
  }
}
