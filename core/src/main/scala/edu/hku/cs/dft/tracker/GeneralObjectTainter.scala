package edu.hku.cs.dft.tracker

/**
  * Created by jianyu on 4/21/17.
  */

/**
  * This class class uses reflection to add taint to a specific variable
  * TODO: finish it in the future
 */

abstract class GeneralObjectTainter {
  def setVariableTaint(vs: String, obj: Object, any: Any): Unit

  def getVariableTaint(vs: String, obj: Object): CombinedTaint[_]
}

/**
  * [[ObjectTaintedVariable]] is used to tell the system which tag should be added to a specific variable
  * For example, "username" -> 1 means that 1 should be added as taint to username filed in the object
  * If a [[ObjectTaintedVariable]] appears instead of a specific taint, which means that the filed is a
  * object instead of a primitive, then the object filed will be added with the taint according to this ObjectTaintedVariable
*/
case class ObjectTaintedVariable(variableArr: Map[String, Any])