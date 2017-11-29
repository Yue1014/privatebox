package edu.hku.cs.dft.examples.performance

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianyu on 5/3/17.
  */
object TwitterHot {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val twitterfile = args(0)

    val sc = new SparkContext(conf)

    var file = sc.textFile(twitterfile, 10)

    val trace = args.length > 1 && args(1).equals("true")

    val backtrace = args.length > 2 && args(2).equals("true")

    if (backtrace) {
      file = file.taint(t => {
        if (t.contains("#FAKENEWS")) {
          1
        } else {
          -1
        }
      })
    }

    if (trace)
      file = file.zipWithUniqueId().taint(t => {
      (t._2, -1)
    }).map(_._1)

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

    val time_group = top_words.flatMapValues(t => t)

    if (trace)
      time_group.zipWithTaint().map{
        case ((time, (word, count)), taint_tuple) =>
          val taint_arr = taint_tuple.asInstanceOf[(_, _)]._2
            .asInstanceOf[(_, _)]._2
            .asInstanceOf[Array[Object]]
//            .asInstanceOf[CombinedTaint[_]].iterator.toArray
          val l = if (taint_arr == null)
            0
          else
            taint_arr.length
          (time, word, count, l)
      }.saveAsObjectFile("twitter_out")
    else if (backtrace) {
      val k =
      time_group.zipWithTaint().filter {
        case (((time, (word, count)), taint)) =>
          val t =taint.asInstanceOf[(_, (_, _))]._2._2
          t != null
      }.count()
      println(k + ":")
    }
    else
      time_group.saveAsObjectFile("twitter_out")

    readLine()

    sc.stop()
  }
}
