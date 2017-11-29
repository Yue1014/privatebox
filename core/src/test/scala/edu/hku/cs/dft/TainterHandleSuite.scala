package edu.hku.cs.dft

import java.io._

import edu.columbia.cs.psl.phosphor.runtime.Taint
import edu.hku.cs.dft.tracker.{CombinedTaint, IntTainter, ObjectTainter, TainterHandle}

/**
  * Created by jianyu on 4/17/17.
  */
object TainterHandleSuite {
  def main(args: Array[String]): Unit = {
    val tainterHandle: TainterHandle = new ObjectTainter
    val taintedOne = tainterHandle.setTaint(1, 1)
    val taintedTwo = tainterHandle.setTaint(2, 2)
    val taintedThree = taintedOne + taintedTwo
    val originalTaint = tainterHandle.getTaint(taintedOne)
    val propogateTaint = tainterHandle.getTaint(taintedThree)
    val a = 1
    val fileOutputStream = new ObjectOutputStream(new FileOutputStream(new File("/tmp/o.tmp")))
    fileOutputStream.writeObject(propogateTaint)
    val obj = new ObjectInputStream(new FileInputStream(new File("/tmp/o.tmp"))).readObject()
    val getThree = tainterHandle.setTaint(3, obj)
    val treeTaint = tainterHandle.getTaint(getThree)
    val k = 1
  }
}
