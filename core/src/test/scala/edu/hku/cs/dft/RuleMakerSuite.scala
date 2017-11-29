package edu.hku.cs.dft

import edu.hku.cs.dft.optimization.RuleMaker

/**
  * Created by jianyu on 3/21/17.
  */
object RuleMakerSuite {
  def main(args: Array[String]): Unit = {
    val typeInfo = (Int, Int, ((Double, Double), Int))
    val kvInfo = ((Int, Int), Double)
    val rule = RuleMaker.makeOneToOneRuleFromTypeInfo(typeInfo)
    assert(rule == Map(1 -> List(1), 2 -> List(2), 3 -> List(3), 4 -> List(4), 5 -> List(5)))
    val len = RuleMaker.typeInfoLength(kvInfo._1)
    assert(len == 2)
  }
}
