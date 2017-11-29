package edu.hku.cs.dft.tracker

import edu.columbia.cs.psl.phosphor.runtime.TaintChecker
import edu.columbia.cs.psl.phosphor.runtime.TaintChecker.{CheckFuncInt, CheckFuncObj}
import edu.hku.cs.dft.network.NettyEndpoint

/**
  * Created by jianyu on 9/7/17.
  */
trait LocalChecker extends NettyEndpoint with Serializable{

  val checkFuncInt: Option[Int => Unit] = None

  val checkFuncObj: Option[Object => Unit] = None

  def setCheckFunc(): Unit = {
    if (checkFuncInt.isDefined) {
      TaintChecker.setCheckerFunc(new CheckFuncInt {
        override def checkTag(i: Int): Unit = checkFuncInt.get(i)
      })
    }
    if (checkFuncObj.isDefined) {
      TaintChecker.setCheckerFunc(new CheckFuncObj {
        override def checkTag(o: Object): Unit = checkFuncObj.get(o)
      })
    }
  }
}
