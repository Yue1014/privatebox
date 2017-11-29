package edu.hku.cs.dft.examples.dp

import java.io.{File, PrintWriter}

import breeze.linalg.DenseVector
import edu.hku.cs.dft.dp._
import org.apache.spark.sql.SparkSession

object MedicalQuery {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MedicalQuery")
      .config("spark.dft.tracking.mode", "full")
      .getOrCreate()

    // partition, splits
    val file = args(0)
    val par = args(1).toInt
    val split = args(2).toInt
    val input = spark.sparkContext.textFile(file, par)
    val formatedInput = input.map(t => {
      val sarr = t.split(",")
      // level(0) // id(1) // weight(2) // clin(3) // wbco(4) // lpcc(5) // illd(6) // biwt(7) // hcir(8) // wght(9)
      // lgth(10) // temp(11) // cdip(12) // hrat(13)
      // country(14) // age(15) // rr(16) // cprot(17) // hlt(18) // implication(19)
      // IllID + age + taint + sym+1 + sym+2 + sym+3
      (sarr(6).toInt, sarr(15).toInt, sarr(0).toInt, sarr(44).toInt, sarr(45).toInt, sarr(46).toInt)
      // illd, age, other accuracy
    })

    val result_pre = formatedInput.taint(t => (0, t._3, 0, 0, 0, 0)).map(t => (t._1, (t._2, t._4, t._5, t._6)))
      .groupByKey().map(t => {
      val key = t._1
      val group = t._2
      val r = group.reduceLeft((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
      val n = group.size
      (key, r._1 / n, r._2 / n, r._3 / n, r._4 / n)
    })

    val result = result_pre.zipWithTaint().collectSplit(split)

    val tv = result_pre.sortBy(_._1).collect()
    val none_r = DPAggregator.dp_aggregate(result, new NoneDPModel)
    val delta = DPAggregator.delta(result)

    val gupt_r = DPAggregator.dp_aggregate(result, new GUPTDPModel(split))
    println("gupt:: " + DPAggregator.noise_magnitude(gupt_r, none_r, delta))
    println("gupt:: " + DPAggregator.noise_accuracy(gupt_r, tv, delta))

    val naive_r = DPAggregator.dp_aggregate(result, new NaiveDPModel(split))
    println("naive::" + DPAggregator.noise_magnitude(naive_r, none_r, delta))
    println("naive::" + DPAggregator.noise_accuracy(naive_r, tv, delta))

    val combine_r = DPAggregator.dp_aggregate(result, new CombinedDPModel)
    println("combine:: " + DPAggregator.noise_magnitude(combine_r, none_r, delta))
    println("combine:: " + DPAggregator.noise_accuracy(combine_r, tv, delta))

    val split_r = DPAggregator.dp_aggregate(result, new SplitDPModel(split)).toVector.sortBy {
      case p: Product => p.productElement(0)
      case _ => throw new IllegalArgumentException()
    }(AnyOrder)
    println("split:: " + DPAggregator.noise_magnitude(split_r, none_r, delta))
    println("split:: " + DPAggregator.noise_accuracy(split_r, tv, delta))

    spark.stop()

  }
}
