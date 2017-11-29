package edu.hku.cs.dft.tracker

import edu.hku.cs.dft.DFTEnv

/**
  * [[TupleTainter]] is a more flexible api to add taint, the tuple shuld be as the same
  * structure as the data: if obj is tuple2, then taint should be tuple2
  */
object TupleTainter {

  private val tainter = if (TainterHandle.trackingTaint == TrackingTaint.ObjTaint) new ObjectTainter else new IntTainter

  private def getBaseClass(obj: Any): (Boolean, Int) = {
    obj match {
      case p: Product => (true, p.productArity)
      case _ => (false, 0)
    }
  }

  def getTaintOne[T](obj: T): Any = {
    obj match {
      case v: Int => tainter.getTaint(v)
      case v: Long => tainter.getTaint(v)
      case v: Double => tainter.getTaint(v)
      case v: Float => tainter.getTaint(v)
      case v: Short => tainter.getTaint(v)
      case v: Boolean => tainter.getTaint(v)
      case v: Byte => tainter.getTaint(v)
      case v: Char => tainter.getTaint(v)
      // we should show the taint of the all object
      // also array should be consider
      case v: Iterable[_] => v.map(getTaintOne).toList
      case v: Iterator[_] => v.map(getTaintOne).toList
      case v: Array[_] => v.map(getTaint).toList
      case v: Object => tainter.getTaint(v)
      case null => null
      case _ => throw new Exception("type mismatch type " + obj.getClass.getSimpleName)
    }
  }

  // TODO: Other Basic Collection of data ?
  private def setOne[T](obj: T, tag: Any): T = {
    // TODO check if tag is bypassed
    if (tag == -1 || tag == null) {
      obj
    } else {
      val r = obj match {
        case v: Int => tainter.setTaint(v, tag)
        case v: Long => tainter.setTaint(v, tag)
        case v: Double => tainter.setTaint(v, tag)
        case v: Float => tainter.setTaint(v, tag)
        case v: Short => tainter.setTaint(v, tag)
        case v: Boolean => tainter.setTaint(v, tag)
        case v: Byte => tainter.setTaint(v, tag)
        case v: Char => tainter.setTaint(v, tag)
        case arr: Array[_] => tainter.setTaint(arr, tag)
        case it: Iterator[Any] => it.map(t => setOne(t, tag)).asInstanceOf[T]
        case it: Iterable[Any] => it.map(t => setOne(t, tag)).asInstanceOf[T]
        case ob: Object =>
          if (DFTEnv.objectTainter.isDefined) {
            ob match {
              case i: Integer =>
              case _ => tainter.setTaint(ob, tag)
            }

          } else {
            tainter.setTaint(ob, tag)
          }
      }
      r.asInstanceOf[T]
    }
  }

  def setTaint[T, M](obj: T, taint: M): T = {
    if (getBaseClass(obj) != getBaseClass(taint)) {
      // Can be any obj
      // if (!taint.isInstanceOf[Int] || !taint.isInstanceOf[Integer]) throw new Exception("type not match")
      val tag = taint
      if (tag == -1 || tag == null) {
        return obj
      }
      obj match {
        case (_1, _2) => (setTaint(_1, tag), setTaint(_2, tag)).asInstanceOf[T]
        case (_1, _2, _3) => (setTaint(_1, tag), setTaint(_2, tag), setTaint(_3, tag)).asInstanceOf[T]
        case (_1, _2, _3, _4) => (setTaint(_1, tag), setTaint(_2, tag), setTaint(_3, tag), setTaint(_4, tag)).asInstanceOf[T]
        case (_1, _2, _3, _4, _5) => (setTaint(_1, tag), setTaint(_2, tag), setTaint(_3, tag), setTaint(_4, tag),
                                      setTaint(_5, tag)).asInstanceOf[T]
        case (_1, _2, _3, _4, _5, _6) => (setTaint(_1, tag), setTaint(_2, tag), setTaint(_3, tag), setTaint(_4, tag),
                                          setTaint(_5, tag), setTaint(_6, tag)).asInstanceOf[T]
        case _ => setOne(obj, tag)
      }
    } else {
      val r = obj match {
        case (_1, _2) =>
          val tupleTaint = taint.asInstanceOf[(_, _)]
          (setTaint(_1, tupleTaint._1),
           setTaint(_2, tupleTaint._2))
        case (_1, _2, _3) =>
          val tupleTaint = taint.asInstanceOf[(_, _, _)]
          (setTaint(_1, tupleTaint._1),
           setTaint(_2, tupleTaint._2),
           setTaint(_3, tupleTaint._3))
        case (_1, _2, _3, _4) =>
          val tupleTainter = taint.asInstanceOf[(_, _, _, _)]
          (setTaint(_1, tupleTainter._1),
          setTaint(_2, tupleTainter._2),
          setTaint(_3, tupleTainter._3),
          setTaint(_4, tupleTainter._4))
        case (_1, _2, _3, _4, _5) =>
          val tupleTainter = taint.asInstanceOf[(_, _, _, _, _)]
          (setTaint(_1, tupleTainter._1),
            setTaint(_2, tupleTainter._2),
            setTaint(_3, tupleTainter._3),
            setTaint(_4, tupleTainter._4),
            setTaint(_5, tupleTainter._5))
        case (_1, _2, _3, _4, _5, _6) =>
          val tupleTainter = taint.asInstanceOf[(_, _, _, _, _, _)]
          (setTaint(_1, tupleTainter._1),
            setTaint(_2, tupleTainter._2),
            setTaint(_3, tupleTainter._3),
            setTaint(_4, tupleTainter._4),
            setTaint(_5, tupleTainter._5),
            setTaint(_6, tupleTainter._6))
        case _ =>
          setOne(obj, taint)
      }
      r.asInstanceOf[T]
    }

  }

  def getTaint[T](obj: T): Any = {
    obj match {
      case (_1, _2) => (getTaint(_1), getTaint(_2))
      case (_1, _2, _3) => (getTaint(_1), getTaint(_2), getTaint(_3))
      case (_1, _2, _3, _4) => (getTaint(_1), getTaint(_2), getTaint(_3), getTaint(_4))
      case (_1, _2, _3, _4, _5) => (getTaint(_1), getTaint(_2), getTaint(_3), getTaint(_4), getTaint(_5))
      case _ => getTaintOne(obj)
    }
  }
}
