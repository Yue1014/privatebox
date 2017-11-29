package edu.hku.cs.dft.interface


/**
  * Created by jianyu on 3/20/17.
  */

/**
  * A [[TaintRuleTranslator]] translate a T => Any result(Int Tuple or Array) rule to a Map[Int, f']
*/

//TODO set array taint
object TaintRuleTranslator {

  var tempTranslate: Map[Int, Int] = Map()

  var tempIndex: Int = 0

  def translate(r: Any): Map[Int, Int] = {
    tempTranslate = Map()
    tempIndex = 0
    translateHelper(r)
    tempTranslate
  }

  private def translateHelper(r: Any): Unit = {
    r match {
      case p: Product => p.productIterator.foreach(translateHelper)
      case _ => translateOne(r)
    }
  }

  private def translateOne(r: Any): Unit = {
    tempIndex += 1
    r match {
      case i: Int => tempTranslate += tempIndex -> i
      case _ => throw new Exception("Wrong type of tag")
    }
  }

}
