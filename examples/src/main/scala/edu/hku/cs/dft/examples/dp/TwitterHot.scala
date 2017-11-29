package edu.hku.cs.dft.examples.dp

import edu.hku.cs.dft.dp._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by jianyu on 5/3/17.
  */
object TwitterHot {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.set("spark.dft.tracking.mode", "full")

    val twitterfile = args(0)

    val partitions = args(1).toInt

    val splits = args(2).toInt


    val sc = new SparkContext(conf)

    var file = sc.textFile(twitterfile, partitions)

    file = file.taint(t => {
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

    val to = file.map(
      line => {
        val list = line.split(",")
        if(list.size > 4){
          val time = list(0).split(" ")(3).split(":")(1)
          val text = list(3)
          (time, text)
        }
        else{
          ("15:02:45","  ")
        }
      }

    )

    val file1 = to.groupByKey()

    val top_words = file1.map(t => {
      val words = t._2.flatMap(_.trim.split(" "))
      val sorted_arr = words.map(t => (t, if (t.length > 0) (t.charAt(0).toInt | 1) & 1 else 1))
        .groupBy(_._1).map(l => (l._1, l._2.map(_._2).sum))
        .toArray
        .sortBy(_._2)
      (t._1, sorted_arr)
    })

    val time_group = top_words.flatMapValues(t => t).map(t => (t._1 + t._2._1, t._2._2))

    val result = time_group.zipWithTaint().collectSplit(splits)

    val tv = time_group.collect().sortBy(_._1)

    val none_r = DPAggregator.dp_aggregate(result, new NoneDPModel)
    val delta = DPAggregator.delta(result)

    val gupt_r = DPAggregator.dp_aggregate(result, new GUPTDPModel(splits))
    println("gupt:: " + DPAggregator.noise_magnitude(gupt_r, none_r, delta).asInstanceOf[(String, Double)]._2)
    println("gupt:: " + DPAggregator.noise_accuracy(gupt_r, tv, delta).asInstanceOf[(String, Double)]._2)

    val naive_r = DPAggregator.dp_aggregate(result, new NaiveDPModel(splits))
    println("naive:: " + DPAggregator.noise_magnitude(naive_r, none_r, delta).asInstanceOf[(String, Double)]._2)
    println("naive:: " + DPAggregator.noise_accuracy(naive_r, tv, delta).asInstanceOf[(String, Double)]._2)

    val combine_r = DPAggregator.dp_aggregate(result, new CombinedDPModel)
    println("combine:: " + DPAggregator.noise_magnitude(combine_r, none_r, delta).asInstanceOf[(String, Double)]._2)
    println("combine:: " + DPAggregator.noise_accuracy(combine_r, tv, delta).asInstanceOf[(String, Double)]._2)

    val split_r = DPAggregator.dp_aggregate(result, new SplitDPModel(splits))
    println("split:: " + DPAggregator.noise_magnitude(split_r, none_r, delta).asInstanceOf[(String, Double)]._2)
    println("split:: " + DPAggregator.noise_accuracy(split_r, tv, delta).asInstanceOf[(String, Double)]._2)


    readLine()

    sc.stop()
  }
}
