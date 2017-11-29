package edu.hku.cs.dft.tracker

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
/**
  * Created by jianyu on 3/10/17.
  */

object DFTUtils {

  def getTypeTag(obj: Any): Any = {
    getTypeHelper(obj)
  }

  def getTypeHelper[T: ClassTag](obj: T): Any = {
    obj match {
      case (_1, _2) => (getTypeHelper(_1), getTypeHelper(_2))
      case (_1, _2, _3) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3))
      case (_1, _2, _3, _4) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4))
      case (_1, _2, _3, _4, _5) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5))
      case (_1, _2, _3, _4, _5, _6) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6))
      case (_1, _2, _3, _4, _5, _6, _7) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7))
      case (_1, _2, _3, _4, _5, _6, _7, _8) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11), getTypeHelper(_12))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11), getTypeHelper(_12), getTypeHelper(_13))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11), getTypeHelper(_12), getTypeHelper(_13), getTypeHelper(_14))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11), getTypeHelper(_12), getTypeHelper(_13), getTypeHelper(_14), getTypeHelper(_15))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11), getTypeHelper(_12), getTypeHelper(_13), getTypeHelper(_14), getTypeHelper(_15), getTypeHelper(_16))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11), getTypeHelper(_12), getTypeHelper(_13), getTypeHelper(_14), getTypeHelper(_15), getTypeHelper(_16), getTypeHelper(_17))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11), getTypeHelper(_12), getTypeHelper(_13), getTypeHelper(_14), getTypeHelper(_15), getTypeHelper(_16), getTypeHelper(_17), getTypeHelper(_18))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11), getTypeHelper(_12), getTypeHelper(_13), getTypeHelper(_14), getTypeHelper(_15), getTypeHelper(_16), getTypeHelper(_17), getTypeHelper(_18), getTypeHelper(_19))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11), getTypeHelper(_12), getTypeHelper(_13), getTypeHelper(_14), getTypeHelper(_15), getTypeHelper(_16), getTypeHelper(_17), getTypeHelper(_18), getTypeHelper(_19), getTypeHelper(_20))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20, _21) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11), getTypeHelper(_12), getTypeHelper(_13), getTypeHelper(_14), getTypeHelper(_15), getTypeHelper(_16), getTypeHelper(_17), getTypeHelper(_18), getTypeHelper(_19), getTypeHelper(_20), getTypeHelper(_21))
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20, _21, _22) => (getTypeHelper(_1), getTypeHelper(_2), getTypeHelper(_3), getTypeHelper(_4), getTypeHelper(_5), getTypeHelper(_6), getTypeHelper(_7), getTypeHelper(_8), getTypeHelper(_9), getTypeHelper(_10), getTypeHelper(_11), getTypeHelper(_12), getTypeHelper(_13), getTypeHelper(_14), getTypeHelper(_15), getTypeHelper(_16), getTypeHelper(_17), getTypeHelper(_18), getTypeHelper(_19), getTypeHelper(_20), getTypeHelper(_21), getTypeHelper(_22))
      case null => null
      case _ => obj.getClass.getSimpleName
    }
  }

  def compressTaintMap(map: Map[Int, Int]): Map[Int, Int] = {
    val found = map.find(_._2 != 0)
    var r: Map[Int, Int] = Map()
    found.foreach(rr => {
      r += rr
    })
    r
  }

  def decomposeTaint(tag: Int): List[Int] = {
    var seq = List[Int]()
    for (i <- 0 until 30) {
      if ((tag & (1 << i)) != 0)
        seq = (i + 1) :: seq
    }
    seq
  }

  def markPositionToMap(filter: Map[Int, Any]): Map[Int, Any => Any] = {
    filter.map(pair => {
      (pair._1, (_: Any) => pair._2)
    })
  }

  def nameValid(name: String): Boolean = name != "" && name != null

}