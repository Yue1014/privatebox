package edu.hku.cs.dft.debug

import edu.hku.cs.dft.tracker.CombinedTaint
import org.apache.spark.rdd.RDD

/**
  * Created by jianyu on 4/27/17.
  */
class CoverageChecker {
  var inputList: Map[Int, RDD[_]] = Map()
  var rddIndex = 0

  def tag_input(rdd: RDD[_]): RDD[_] = {
    rddIndex += 1
    val withIndexRDD = rdd.zipWithIndex()
    inputList += rddIndex -> withIndexRDD
    withIndexRDD.taint(t => {
      (rddIndex.toString + t._2, 0)
    }).map(_._1)
  }

  def check_output(rdd: RDD[_]): List[RDD[_]] = {
    // all taint tuple
    var missList: List[RDD[_]] = List()
    val inputTaint = rdd.zipWithTaint().flatMap(t => t._2.asInstanceOf[Product].productIterator
      .flatMap(t => t.asInstanceOf[CombinedTaint[_]]))
      .map(_.asInstanceOf[String])
      .distinct()
    for (i <- 1 to rddIndex) {
      val miss = inputList(i).asInstanceOf[RDD[(_, Long)]].map(_._2.toString.substring(1)).subtract(inputTaint)
      if (miss.count() != 0)
        missList = miss :: missList
    }
    missList
  }


}
