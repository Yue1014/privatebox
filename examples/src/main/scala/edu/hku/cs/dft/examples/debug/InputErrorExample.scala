package edu.hku.cs.dft.examples.debug



import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jianyu on 4/13/17.
  */
object InputErrorExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("input error example")
      .set("spark.dft.tracking.mode", "debug")
    val sc = new SparkContext(conf)

    val textFile = if (args.length > 0) args(0) else throw new IllegalArgumentException("not enough argument")

    val text = sc.textFile(textFile)

    // some data is not int

    // input format should be:
    // error
    // level, time, message

    val record = text.map(t => {
      var splitStrings = t.split("\\s+")
      val formater = new SimpleDateFormat("yyyy-MM-dd")
      val date = formater.parse(splitStrings(1))
      (splitStrings(0), date, splitStrings(2))
    })

    val filter_result = record.filter(t => t._1 == "ERROR" && t._2.getYear > 2015)
    filter_result.collect()
    sc.stop()

  }

}
