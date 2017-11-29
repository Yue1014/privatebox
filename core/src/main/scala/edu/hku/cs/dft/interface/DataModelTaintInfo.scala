package edu.hku.cs.dft.interface

/**
  * Created by jianyu on 3/20/17.
  */

/**
  * A [[DataModelTaintInfo]] is used to tell the worker if a datamodel is tainted and how should a data model be
  * tainted, the initial thought is to add taint function and indicator
  * A controller that control if a executor should be in tracking mode should also be added
*/

case class DataModelTaintInfo[T](tainted: Boolean, taintFunc: T => Any)
