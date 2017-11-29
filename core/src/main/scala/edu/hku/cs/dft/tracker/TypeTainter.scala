
package edu.hku.cs.dft.tracker

/**
  * Created by jianyu on 3/3/17.
  */

/**
  * A [[BaseTainter]] provide basic variables and functions to add
  * Taints to a tuple, should be override by its subclasses
*/

abstract class BaseTainter {
  val TAINT_START_KEY : Int = 1
  val TAINT_START_VALUE : Int = 0x4fffffff + 1

  def setTaint[T](obj: T): T
  def getTaintList(obj: Any): Map[Int, Any]
  def getTaintAndReturn[T](obj: T): T

}

class TaintException(string: String) extends Exception {}