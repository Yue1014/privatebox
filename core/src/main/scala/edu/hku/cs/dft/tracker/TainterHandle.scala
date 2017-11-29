package edu.hku.cs.dft.tracker

import java.lang.reflect.Field
import java.util

import edu.columbia.cs.psl.phosphor.{Configuration, TaintUtils}
import edu.columbia.cs.psl.phosphor.runtime.{MultiTainter, Taint, Tainter}
import edu.hku.cs.dft.DFTEnv
import edu.hku.cs.dft.tracker.TrackingTaint.TrackingTaint

import scala.collection.mutable.HashMap

/**
  * Created by jianyu on 4/17/17.
  */

// abstract handle for lower tainter

class CombinedTaint[T](taint: T) extends Iterable[Any] with Serializable{

  private val tt: T = taint

  def getTaint: T = tt

  override def iterator: Iterator[_] = {
    tt match {
      case null =>
        List(0).toIterator
      case ta: Int =>
        var rs: List[Int] = List()
        for (j <- 0 to 31) {
          if ((ta & (1 << j)) != 0) {
            rs = (1 << j) :: rs
          }
        }
        rs.toIterator
      case taint: Taint[_] =>
        if (tt != null)
          taint.getOriginalTag.toIterator
        else
          Iterator.empty
      case _ =>
        None.toIterator
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    tt match {
      case int: Int =>
        obj match {
          case taint: Int => taint == int
          case _ => false
        }
      case t: Taint[_] =>
        obj match {
          case taint: Taint[_] => taint.lbl == t.lbl
          case _ => false
        }
      case _ => false
    }
  }

  def NonNull(): Boolean = tt != 0 && tt != null

}

/**
  * A [[TainterHandle]] is used to set/set taint to/from a data
*/

trait TainterHandle {

  val MAX_TAINT = 0

  def setFieldTaint(obj: Object, taint: Any, field: String): Object = {
      val f: Field = obj.getClass.getField(field + TaintUtils.TAINT_FIELD)
      f.set(obj, taint)
      obj
  }

  def setTaint[T](anyRef: T, taint: Any): T

  def getWrapTaint(any: Any): CombinedTaint[_]

  def getTaint(any: Any): Any

}

class ObjectTainter extends TainterHandle {

  override val MAX_TAINT: Int = 1 << 31

  private def setObjTaint[T](anyRef: T, taint: Any): T = {
    if (taint == null || taint == -1)
      anyRef
    else {
      val ta = if (DFTEnv.conf().shuffleOpt == ShuffleOpt.CacheTag) {
        val hashcode = taint.hashCode()
        TainterHandle.taintMap.get().get(hashcode, taint).asInstanceOf[Taint[Object]]
      } else {
        new Taint(taint)
      }
      setObjTaint(anyRef, ta)
      // change it for saving space
      /*val r = anyRef match {
        case int: Int => MultiTainter.taintedInt(int, taint)
        case short: Short => MultiTainter.taintedShort(short, taint)
        case long: Long => MultiTainter.taintedLong(long, taint)
        case double: Double => MultiTainter.taintedDouble(double, taint)
        case float: Float => MultiTainter.taintedFloat(float, taint)
        case char: Char => MultiTainter.taintedChar(char, taint)
        case byte: Byte => MultiTainter.taintedByte(byte, taint)
        case boolean: Boolean => MultiTainter.taintedBoolean(boolean, taint)
        case arr: Array[Int] => MultiTainter.taintedIntArray(arr, taint)
        case arr: Array[Short] => MultiTainter.taintedShortArray(arr, taint)
        case arr: Array[Long] => MultiTainter.taintedLongArray(arr, taint)
        case arr: Array[Double] => MultiTainter.taintedDoubleArray(arr, taint)
        case arr: Array[Float] => MultiTainter.taintedFloatArray(arr, taint)
        case arr: Array[Char] => MultiTainter.taintedCharArray(arr, taint)
        case arr: Array[Byte] => MultiTainter.taintedByteArray(arr, taint)
        case arr: Array[Boolean] => MultiTainter.taintedBooleanArray(arr, taint)
        case null => null
        case obj =>
          MultiTainter.taintedObject(obj, new Taint(taint))
          obj
        case _ => throw new IllegalArgumentException("type mismatch")
      }
      r.asInstanceOf[T]*/
    }
  }

  private def setObjTaint[T](anyRef: T, taint: Taint[_]): T = {
    if (taint == null)
      anyRef
    else {
      val r = anyRef match {
        case int: Int => MultiTainter.taintedInt(int, taint)
        case short: Short => MultiTainter.taintedShort(short, taint)
        case long: Long => MultiTainter.taintedLong(long, taint)
        case double: Double => MultiTainter.taintedDouble(double, taint)
        case float: Float => MultiTainter.taintedFloat(float, taint)
        case char: Char => MultiTainter.taintedChar(char, taint)
        case byte: Byte => MultiTainter.taintedByte(byte, taint)
        case boolean: Boolean => MultiTainter.taintedBoolean(boolean, taint)
        case arr: Array[_] => throw new IllegalArgumentException("could not set taint to arr")
        case null => null
        case obj =>
          MultiTainter.taintedObject(obj, taint)
          obj
        case _ => throw new IllegalArgumentException("type mismatch")
      }
      r.asInstanceOf[T]
    }
  }

  override def setTaint[T](anyRef: T, taint: Any): T = {

    taint match {
      case ct: CombinedTaint[_] =>
        setObjTaint(anyRef, ct.getTaint.asInstanceOf[Taint[_]])
      case raw: Taint[_] => setObjTaint(anyRef, raw.lbl)
      case obj: Object => setObjTaint(anyRef, obj)
      case _ => anyRef
    }

  }

  override def getWrapTaint(any: Any): CombinedTaint[Taint[_]] = {
    if (any == null)
      new CombinedTaint(null)
    else {
      val t = any match {
        case s: String =>
          val tt = MultiTainter.getTaint(s)
          if (tt == null) {
            if (s.length > 0)
              MultiTainter.getTaint(s.charAt(0))
            else
              null
          } else {
            tt
          }
        case _ => MultiTainter.getTaint(any)
      }
      if (t != null)
        t.preSerialization()
      new CombinedTaint(t)
    }
  }

  override def getTaint(any: Any): Any = {
    if (any == null)
      null
    else {
      val t = any match {
        case s: String =>
          val tt = MultiTainter.getTaint(s)
          if (tt == null) {
            if (s.length > 0)
              MultiTainter.getTaint(s.charAt(0))
            else
              null
          } else {
            tt
          }
        case _ => MultiTainter.getTaint(any)
      }
      if (t != null) {
        t.preSerialization()
        // do not use any warped data structure here, to save space
        t.lbl
      } else
        null
    }
  }
}

class IntTainter extends TainterHandle {

  private def setIntTaint[T](anyRef: Any, int: Int): T = {
    if (int == -1)
      anyRef.asInstanceOf[T]
    else {
        val r = anyRef match {
          case in: Int => Tainter.taintedInt(in, int)
          case short: Short => Tainter.taintedShort(short, int)
          case long: Long => Tainter.taintedLong(long, int)
          case double: Double => Tainter.taintedDouble(double, int)
          case float: Float => Tainter.taintedFloat(float, int)
          case char: Char => Tainter.taintedChar(char, int)
          case byte: Byte => Tainter.taintedByte(byte, int)
          case boolean: Boolean => Tainter.taintedBoolean(boolean, int)
          case arr: Array[Int] => Tainter.taintedIntArray(arr, int)
          case arr: Array[Short] => Tainter.taintedShortArray(arr, int)
          case arr: Array[Long] => Tainter.taintedLongArray(arr, int)
          case arr: Array[Double] => Tainter.taintedDoubleArray(arr, int)
          case arr: Array[Float] => Tainter.taintedFloatArray(arr, int)
          case arr: Array[Char] => Tainter.taintedCharArray(arr, int)
          case arr: Array[Byte] => Tainter.taintedByteArray(arr, int)
          case arr: Array[Boolean] => Tainter.taintedBooleanArray(arr, int)
          case null => null
          case obj =>
            Tainter.taintedObject(obj, int)
            obj
          case _ => throw new IllegalArgumentException("type mismatch")
        }
        r.asInstanceOf[T]
      }
  }

  override def setTaint[T](anyRef: T, taint: Any): T = {
    taint match {
      case int: Int => setIntTaint(anyRef, int)
      case ct: CombinedTaint[Int] => setIntTaint(anyRef, ct.getTaint)
      case _ => throw new IllegalArgumentException("only int taint is supported")
    }
  }

  override def getWrapTaint(any: Any): CombinedTaint[_] = {
    if (any == null) {
      new CombinedTaint[Int](0)
    } else {
      new CombinedTaint[Int](Tainter.getTaint(any))
    }
  }

  override def getTaint(any: Any): Any = {
    if (any == null)
      0
    else
      Tainter.getTaint(any)
  }

}

class TaintCache {
  val CACHE_SIZE = 10000

  var cache: Array[Object] = new Array[Object](CACHE_SIZE)

  var cache_index: Array[Int] = new Array[Int](CACHE_SIZE)

  def get(index: Int, any: Any): Object = {
    val key = math.abs(index % CACHE_SIZE)
    if (cache_index(key) == index)
      cache(key)
    else {
      cache_index(key) = index
      cache(key) = new Taint(any)
      cache(key)
    }

  }

  def put(index: Int, taint: Object): Unit = {
    cache(index % CACHE_SIZE) = taint
    cache_index(index % CACHE_SIZE) = index
  }

}

object TainterHandle {

  def trackingTaint: TrackingTaint =
    if (edu.columbia.cs.psl.phosphor.Configuration.MULTI_TAINTING)
      TrackingTaint.ObjTaint
    else
      TrackingTaint.IntTaint

  // init this map every time a new shuffle task is executed
  var taintMap: ThreadLocal[TaintCache] = new ThreadLocal[TaintCache]() {
    @Override
    override protected def initialValue: TaintCache = {
      new TaintCache()
    }
  }

  def initMap(): Unit = {
//    if (trackingTaint == TrackingTaint.ObjTaint) {
//      taintMap =
//    }
  }

  def cleanMap(): Unit = {
    taintMap = null
  }

}