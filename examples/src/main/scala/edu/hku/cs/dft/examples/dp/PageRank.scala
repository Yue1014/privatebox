
package edu.hku.cs.dft.examples.dp

import java.io.{File, PrintWriter}

import edu.hku.cs.dft.dp._
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Use a simple word count to test our api
  * Usage: DebuggingTest file
  */

object PageRank {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("pagerank")
      .config("spark.dft.tracking.mode", "full")
      .getOrCreate()

    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter> <partition> <split>")
      System.exit(1)
    }

    val iters = if (args.length > 1) args(1).toInt else 10
    val par = if (args.length > 2) args(2).toInt else 10
    val split = if (args.length > 3) args(3).toInt else 2
    val lines = spark.sparkContext.textFile(args(0), par)
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1), parts(2).toInt)
    }.taint(_._3).map(t => (t._1, t._2)).distinct().groupByKey().cache()
    links.foreach(println)
    var ranks = links.map(t => (t._1, ((t._1.charAt(0) | 1) & 1) * 1.0))

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    println("from here")
    val r = ranks.map(t => {
      val d = if (t._2 > 1)
        1
      else if (t._2 < 0)
        0
      else
        t._2
      (t._1, d)
    }).zipWithTaint().collectSplit(split)

    val trueValue = ranks.map(t => {
      val d = if (t._2 > 1)
        1
      else if (t._2 < 0)
        0
      else
        t._2
      (t._1, d)
    }).collect().sortBy(_._1)

    DPAggregator.dp_pre_save(r, "pagerank.data")

    val delta_f = DPAggregator.delta(r)

    val none_r = DPAggregator.dp_aggregate(r, new NoneDPModel)

    val gupt_r = DPAggregator.dp_aggregate(r, new GUPTDPModel(split))
    println("gupt::" + DPAggregator.noise_magnitude(gupt_r, none_r, delta_f))
    println("gupt::" + DPAggregator.noise_accuracy(gupt_r, trueValue, delta_f))

    val naive_r = DPAggregator.dp_aggregate(r, new NaiveDPModel(split))
    println("naive::" + DPAggregator.noise_magnitude(naive_r, none_r, delta_f))
    println("naive::" + DPAggregator.noise_accuracy(naive_r, trueValue, delta_f))

    val combine_r = DPAggregator.dp_aggregate(r, new CombinedDPModel)
    println("combine::" + DPAggregator.noise_magnitude(combine_r, none_r, delta_f))
    println("combine::" + DPAggregator.noise_accuracy(combine_r, trueValue, delta_f))

    val split_r = DPAggregator.dp_aggregate(r, new SplitDPModel(split))
    println("split::" + DPAggregator.noise_magnitude(split_r, none_r, delta_f))
    println("split::" + DPAggregator.noise_accuracy(split_r, trueValue, delta_f))


//    var writer = new PrintWriter(new File("origin.pagerank"))
//    DPAggregator.dp_aggregate(r, new NoneDPModel).foreach(t => {
//      writer.write(t + "\n")
//    })
//    writer.close()

//    writer = new PrintWriter(new File("gupt.pagerank"))
//    DPAggregator.dp_aggregate(r, new GUPTDPModel(split)).foreach(t => {
//      writer.write(t + "\n")
//    })
//    writer.close()

//    writer = new PrintWriter(new File("naive.pagerank"))
//    DPAggregator.dp_aggregate(r, new NaiveDPModel(split)).foreach(t => {
//      writer.write(t + "\n")
//    })
//    writer.close()

//    writer = new PrintWriter(new File("ours.pagerank"))
//    DPAggregator.dp_aggregate(r, new CombinedDPModel).foreach(t => {
//      writer.write(t + "\n")
//    })
//    writer.close()

//    writer = new PrintWriter(new File("split.pagerank"))
//    DPAggregator.dp_aggregate(r, new SplitDPModel(split)).foreach(t => {
//      writer.write(t + "\n")
//    })
//    writer.close()
    readLine()
    spark.stop()
  }
}
