package edu.hku.cs.dft

import edu.hku.cs.dft.interface.TaintRuleTranslator


/**
  * Created by jianyu on 3/20/17.
  */
object TaintRuleTranslatorSuite {

  def taintFunc(f: Any): Any ={
    ((3, 2), 1)
  }

  def compare(g: Map[Int, Int], tg: Map[Int, Int]): Boolean = {
    if (g.size != tg.size)
      false
    else {
      g.foreach(pair => {
        if (!tg.contains(pair._1))
          return false
        if (tg(pair._1) != pair._2)
          return false
      })
    }
    true
  }

  def main(args: Array[String]): Unit = {
    val k = ((1, 2), 3)
    val taintR = taintFunc(k)
    val g = TaintRuleTranslator.translate(taintR)
    val tg = Map(1 -> 3, 2 -> 2, 3 -> 1)
    val same = compare(g, tg)
    assert(same)
  }

}
