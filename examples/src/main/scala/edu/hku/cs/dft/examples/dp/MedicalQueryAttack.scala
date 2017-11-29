package edu.hku.cs.dft.examples.dp

import edu.hku.cs.dft.dp._
import org.apache.spark.sql.SparkSession

object MedicalQueryAttack {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("MedicalQuery")
      .config("spark.dft.tracking.mode", "full")
      .getOrCreate()

    spark.sparkContext.setLogLevel("OFF")

    println("Start Jobs!!")
    // partition, splits
    val file = args(0)
    val par = args(1).toInt
    val split = args(2).toInt

    val select_num = args(3).toDouble

    val select_name = args(4)

    val input = spark.sparkContext.textFile(file, par)

    val formatedInput_name = input.map(t => {
      val sarr = t.split(",")
      // level(0) // id(1) // weight(2) // clin(3) // wbco(4) // lpcc(5) // illd(6) // biwt(7) // hcir(8) // wght(9)
      // lgth(10) // temp(11) // cdip(12) // hrat(13)
      // country(14) // age(15) // rr(16) // cprot(17) // hlt(18) // implication(19)
      // IllID + age + taint + sym+1 + sym+2 + sym+3
      (sarr(1), (sarr(6).toInt, sarr(15).toInt, sarr(0).toInt, sarr(44).toInt, sarr(45).toInt, sarr(46).toInt))
      // illd, age, other accuracy
    })

    val formatedInput_d = formatedInput_name.filter(_._1 != select_name).map(_._2)

    val formatedInput = formatedInput_name.map(_._2)

    val result_pre = formatedInput.taint(t => (0, t._3, 0, 0, 0, 0)).map(t => (t._1, (t._2, t._4, t._5, t._6)))
      .groupByKey().map(t => {
      val key = t._1
      val group = t._2
      val r = group.reduceLeft((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
      val n = group.size
      (key.toDouble, r._1.toDouble / n, r._2.toDouble / n, r._3.toDouble / n, r._4.toDouble / n)
    })

    val result_pre_d = formatedInput_d.taint(t => (0, t._3, 0, 0, 0, 0)).map(t => (t._1, (t._2, t._4, t._5, t._6)))
      .groupByKey().map(t => {
      val key = t._1
      val group = t._2
      val r = group.reduceLeft((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
      val n = group.size
      (key.toDouble, r._1.toDouble / n, r._2.toDouble / n, r._3.toDouble / n, r._4.toDouble / n)
    })

    val result = result_pre.zipWithTaint().collectSplit(split)

    val result_d = result_pre_d.zipWithTaint().collectSplit(split)

    println("result:::")

    print("origin:")
    result_pre.sortBy(_._1).collect().filter(_._1 == select_num.toInt).foreach(println)

    print("origin_d:")
    result_pre_d.sortBy(_._1).collect().filter(_._1 == select_num.toInt).foreach(println)

    print("edp:")
    DPAggregator.dp_aggregate(result, new SplitDPModel(split)).filter(t => t match {
      case (_1, _, _, _, _) =>
        _1.asInstanceOf[Double] == select_num
      case _ => throw new IllegalArgumentException()
    }
    ).foreach(println)

    print("edp_d:")
    DPAggregator.dp_aggregate(result_d, new SplitDPModel(split)).filter(t => t match {
      case (_1, _, _, _, _) =>
        _1.asInstanceOf[Double] == select_num
      case _ => throw new IllegalArgumentException()
    }).foreach(println)

    print("gupt:")
    DPAggregator.dp_aggregate(result, new GUPTDPModel(split)).filter(t => t match {
      case (_1, _, _, _, _) =>
        _1.asInstanceOf[Double] == select_num
      case _ => throw new IllegalArgumentException()
    }
    ).foreach(println)

    print("gupt_d:")
    DPAggregator.dp_aggregate(result_d, new GUPTDPModel(split)).filter(t => t match {
      case (_1, _, _, _, _) =>
        _1.asInstanceOf[Double] == select_num
      case _ => throw new IllegalArgumentException()
    }).foreach(println)

    spark.stop()

  }
}
