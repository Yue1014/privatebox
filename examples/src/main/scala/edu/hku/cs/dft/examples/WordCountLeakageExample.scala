package edu.hku.cs.dft.examples

/**
  * Created by jianyu on 4/9/17.
  */

import org.apache.spark._
import java.io._


object WordCountLeakageExample {

  def main(args: Array[String]) {

    if (args.length < 1) throw new IllegalArgumentException("no enough argument")
    val input = args(0)
    val leak = if (args.length > 1) args(1).toBoolean else false

    if (leak) println("dump data to disk")
    def mm(in: String): (String, Int) = if (leak) {
      try {
        val fileOutputStream = new FileWriter(new File("leak.txt"), true)
        fileOutputStream.write(in + "\n")
        fileOutputStream.close()
      }
      catch {
        case e: Exception => println(e.getStackTrace)
      }
      (in, 1)
    } else (in, 1)


    val conf = new SparkConf().setAppName("wordCountApp")
    val sc = new SparkContext(conf)

    val text =  sc.textFile(input, 40)
    val words = text.flatMap(line => line.split(" "))
    val wc = words.map(mm).reduceByKey{case (x, y) => x + y}
    //save to .txt file locally
    val output = "wiki_out"
    wc.saveAsTextFile(output)

    readLine()
    sc.stop()
  }

}