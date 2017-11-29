package edu.hku.cs.dft

import edu.hku.cs.dft.optimization.RuleCollector
import edu.hku.cs.dft.optimization.RuleCollector.RuleSet


/**
  * Created by max on 18/3/2017.
  */
object RuleCollectorSuite {
  def main(args: Array[String]): Unit = {
    var r: RuleSet = Map()
    var k: RuleSet = Map()
    r += List((1, List(1, 2))) -> 3
    k += List((1, List(1, 2))) -> 5
    val g = RuleCollector.CombineRule(r, k)
    assert(RuleCollector.CombineRule(r, k).head._2 == 8)

    r += List((1, List(1, 2)), (2, List(1))) -> 3
    k += List((1, List(1, 2)), (2, List(2))) -> 5
    var m: RuleSet = Map()
    m += List((1, List(1, 2))) -> 8
    m += List((1, List(1, 2)), (2, List(2))) -> 5
    m += List((1, List(1, 2)), (2, List(1))) -> 3
    val t = RuleCollector.CombineRule(r, k)
    assert(t.size == 3)
    //TODO Run more test

    val testA = Map(List((3,List(2)), (2,List(1)), (1,List(1))) -> 24)
    val testB = Map(List((3,List(2)), (2,List(1)), (1,List(1))) -> 16)
    val testC = RuleCollector.CombineRule(testA, testB)
    val c = 1 + 2
  }
}
