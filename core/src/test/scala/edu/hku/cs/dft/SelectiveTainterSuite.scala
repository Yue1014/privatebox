package edu.hku.cs.dft

import edu.hku.cs.dft.tracker.SelectiveTainter


/**
  * Created by jianyu on 3/14/17.
  */

object SelectiveTainterSuite {

  case class TA(int: Int, k: Int)
  def main(args: Array[String]): Unit = {
    val k = (1, 2, 3)
    val k_2 = (2, 3, 4)
    val selectiveTainter = new SelectiveTainter(Map(), 1)

    val aa = (1, 2)
    val tai = (TA(1, 2), TA(2, 3))

    selectiveTainter.setTaint(k)
    val k_taint = selectiveTainter.setTaint(k_2)
    val taint = selectiveTainter.getTaintList(k_taint)
    val kk = selectiveTainter.setTaintWithTaint(k, taint)
    val taint_after = selectiveTainter.getTaintList(kk)
    val a = (1, 1)
    val b = (TA(1, 2), 1)

//    assert(selectiveTainter.getTaintList(tupleTainted) == Map(1 -> 1, 2-> 2, 3 -> 2))

    val g = List(1, 2, 3, 4).toIterator

    // test for array
    val taintedList = selectiveTainter.setTaintWithTaint((1, List(1, 2)), Map(1 -> 1, 2 -> 2))
    val taint_list = selectiveTainter.getTaintList(taintedList)
    val retaintedList = selectiveTainter.setTaintWithTaint((1, List(1, 2)), taint_list)
    val look = 0
  }
}