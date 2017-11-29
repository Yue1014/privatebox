package edu.hku.cs.dft

import breeze.linalg.DenseVector

/**
  * Created by jianyu on 4/19/17.
  */
object DenseVectorTest {

  def f[T](t: T): T = {
    t match {
      case i: Int => (i + 1).asInstanceOf[T]
      case d: Double => (d + 0.5).asInstanceOf[T]
      case _ => t
    }
  }

  def main(args: Array[String]): Unit = {
    val a = if (false) DenseVector(1, 2, 3) else DenseVector(1.0, 2.0)
    val g = a match {
      case dv: DenseVector[Any] =>
        new DenseVector(dv.data.map(f), dv.offset, dv.stride, dv.length)
      case _ => null
    }
    val k = 1
  }
}
