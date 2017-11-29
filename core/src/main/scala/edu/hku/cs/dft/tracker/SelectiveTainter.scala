package edu.hku.cs.dft.tracker

import edu.hku.cs.dft.DFTEnv

/**
  * Created by jianyu on 3/7/17.
  */

/**
  * A [[SelectiveTainter]] add to taint to a specific record,
  * when a record fullfill some conditions
  * The default rule is in the rule
  * So 0 -> default_func
*/

class SelectiveTainter(filter: Map[Int, Any => Any], defaultTag: Any = -1) extends BaseTainter{

  private var _index = 0

  private var _indexDeps = 0

  private var _deps: Map[Int, Any] = Map()

  private var _positionFilter: Map[Int, Any => Any] = if (filter == null) Map() else filter

  private val tainter = if (TainterHandle.trackingTaint == TrackingTaint.ObjTaint) new ObjectTainter else new IntTainter

  private var shuffle_val: Any = _

  private val COMBINED_STUB = -2

  // if there are no rule for default, just make then untainted
  def defaultFilter: Any => Any = _positionFilter.getOrElse(0, _ => defaultTag)

  def setTaintWithTaint[T](obj: T, filter: Map[Int, Any]): T = {
    setFilter(DFTUtils.markPositionToMap(filter))
    setTaint(obj)
  }

  def setFilter(filter: Map[Int, Any => Any]): SelectiveTainter = {
    _positionFilter = filter
    this
  }

  override def setTaint[T](obj: T): T = {
    _index = 0
    val k = taintAllHelper(obj)
    k
  }

  /**
    * if the the positionFilter is a f: Any => Int, if Int is positive, then tag will be added,
    * or if the tag is zero, then the tag will be clear.
    * if the tag is less than 0, then the tag will not change
    * TODO: Do we need this?
  */
  def taintOne[T](obj: T): T = {
    _index += 1
    val f = _positionFilter.getOrElse(_index, defaultFilter)
    var tag = f(obj)
    if (tag == -1) {
      obj
    } else {
      // TODO: We do not need to read conf file, right?
      if (tag == COMBINED_STUB) {
        tag = _positionFilter.getOrElse(1, defaultFilter)
      }
      val r = tag match {
        case arrTaint: ArrayTaintWrapper =>
          obj match {
            case it: Iterable[_] => it.zip(arrTaint.array).map(t => tainter.setTaint(t._1, t._2))
            case it: Iterator[_] => it.zip(arrTaint.array.iterator).map(t => tainter.setTaint(t._1, t._2))
            case it: Array[_] => it.zip(arrTaint.array).map(t => tainter.setTaint(t._1, t._2))
            case _ => throw new IllegalArgumentException("arr taint w ill-arr obj")
          }
        case _ =>
          obj match {
            // Primitive
            case v: Int => tainter.setTaint(v, tag)
            case v: Long => tainter.setTaint(v, tag)
            case v: Double => tainter.setTaint(v, tag)
            case v: Float => tainter.setTaint(v, tag)
            case v: Short => tainter.setTaint(v, tag)
            case v: Boolean => tainter.setTaint(v, tag)
            case v: Byte => tainter.setTaint(v, tag)
            case v: Char => tainter.setTaint(v, tag)
            case arr: Array[_] => tainter.setTaint(arr, tag)
            case it: Iterator[Any] => it.map(t => TupleTainter.setTaint(t, tag)).asInstanceOf[T]
            case it: Iterable[Any] => it.map(t => TupleTainter.setTaint(t, tag)).asInstanceOf[T]
            case ob: Object => tainter.setTaint(ob, tag)
            case _ => obj
          }
      }
      r.asInstanceOf[T]
    }
  }

  def getOne[T](obj: T): T = {
    _indexDeps += 1
    // Todo: if compression will be needed ?
    val tag = obj match {
      case it: Iterable[_] => ArrayTaintWrapper(it.map(tainter.getTaint).toArray)
      case it: Iterator[_] => ArrayTaintWrapper(it.map(tainter.getTaint).toArray)
      case arr: Array[_] => ArrayTaintWrapper(arr.map(tainter.getTaint))
      case _ => tainter.getTaint(obj)
    }
//    tainter.getTaint(obj)

    // ignore the one with empty tag
    tag match {
      case c: CombinedTaint[_] => if (c.NonNull()) _deps += _indexDeps -> c
      case null =>
      case _ =>
        var ctag = tag
        if (DFTEnv.conf().shuffleOpt == ShuffleOpt.CombinedTag) {
          if (_indexDeps == 1) {
            shuffle_val = tag
          }
          else if (shuffle_val == tag) {
            ctag = COMBINED_STUB
          }
        }
        _deps += _indexDeps -> ctag

    }
    obj
  }

  private def taintAllHelper[T](obj: T): T = {
    obj match {
      /* Product, Scala Allow only 22 elements in a tuple */
      case arr: Array[Product] =>
        val markIndex = _index
        arr.map(ar => {
          _index = markIndex
          taintAllHelper(ar)
        }).copyToArray(arr)
        _index = markIndex
        arr.asInstanceOf[T]
      case (_1, _2) => (taintAllHelper(_1), taintAllHelper(_2)).asInstanceOf[T]
      case (_1, _2, _3) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3)).asInstanceOf[T]
      case (_1, _2, _3, _4) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11), taintAllHelper(_12)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11), taintAllHelper(_12), taintAllHelper(_13)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11), taintAllHelper(_12), taintAllHelper(_13), taintAllHelper(_14)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11), taintAllHelper(_12), taintAllHelper(_13), taintAllHelper(_14), taintAllHelper(_15)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11), taintAllHelper(_12), taintAllHelper(_13), taintAllHelper(_14), taintAllHelper(_15), taintAllHelper(_16)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11), taintAllHelper(_12), taintAllHelper(_13), taintAllHelper(_14), taintAllHelper(_15), taintAllHelper(_16), taintAllHelper(_17)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11), taintAllHelper(_12), taintAllHelper(_13), taintAllHelper(_14), taintAllHelper(_15), taintAllHelper(_16), taintAllHelper(_17), taintAllHelper(_18)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11), taintAllHelper(_12), taintAllHelper(_13), taintAllHelper(_14), taintAllHelper(_15), taintAllHelper(_16), taintAllHelper(_17), taintAllHelper(_18), taintAllHelper(_19)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11), taintAllHelper(_12), taintAllHelper(_13), taintAllHelper(_14), taintAllHelper(_15), taintAllHelper(_16), taintAllHelper(_17), taintAllHelper(_18), taintAllHelper(_19), taintAllHelper(_20)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20, _21) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11), taintAllHelper(_12), taintAllHelper(_13), taintAllHelper(_14), taintAllHelper(_15), taintAllHelper(_16), taintAllHelper(_17), taintAllHelper(_18), taintAllHelper(_19), taintAllHelper(_20), taintAllHelper(_21)).asInstanceOf[T]
      case (_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20, _21, _22) => (taintAllHelper(_1), taintAllHelper(_2), taintAllHelper(_3), taintAllHelper(_4), taintAllHelper(_5), taintAllHelper(_6), taintAllHelper(_7), taintAllHelper(_8), taintAllHelper(_9), taintAllHelper(_10), taintAllHelper(_11), taintAllHelper(_12), taintAllHelper(_13), taintAllHelper(_14), taintAllHelper(_15), taintAllHelper(_16), taintAllHelper(_17), taintAllHelper(_18), taintAllHelper(_19), taintAllHelper(_20), taintAllHelper(_21), taintAllHelper(_22)).asInstanceOf[T]
      case _ => taintOne(obj)
    }
  }

  override def getTaintList(obj: Any): Map[Int, Any] = {
    _indexDeps = 0
    _deps = Map()
    getTaintHelper(obj)
    _deps
  }

  def getTaintHelper[T](obj: T): T = {
    obj match {
      case t: (_, _) => t.productIterator.foreach(getTaintHelper)
      case t: (_, _, _) => t.productIterator.foreach(getTaintHelper)
      case t: (_, _, _, _) => t.productIterator.foreach(getTaintHelper)
      case t: (_, _, _, _, _) => t.productIterator.foreach(getTaintHelper)
      case t: (_, _, _, _, _, _, _) => t.productIterator.foreach(getTaintHelper)
//      case product: Product => product.productIterator.foreach(getTaintHelper)
      case _ => getOne(obj)
    }
    obj
  }

  override def getTaintAndReturn[T](obj: T): T = {
    throw new TaintException("Not implemented")
  }
}
