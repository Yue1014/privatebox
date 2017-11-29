package edu.hku.cs.dft.tracker

import edu.hku.cs.dft.{CheckerConf, ConfEnumeration, TrackingMode}
import edu.hku.cs.dft.tracker.TrackingType.TrackingType
import org.apache.spark.rdd.RDD
import org.apache.spark.{InterruptibleIterator, Partition, TaskContext}


/**
  * Created by jianyu on 3/7/17.
  */

/**
  * A [[TrackingPolicy]] is to determined how to add / remove tag to data,
  * And how to handle tag propagation between cluster
  *
  * A [[TrackingPolicy]] determines following behaviours:
  * 1) How to add / remove tag in a new operator
  * 2) How to add tags to newly emitted key-value pair
  * 3) How to add tags to the input files(like HDFS?)
  * 4) How to deal with tag propagation between machines
  * 5) Which tags to be added when propagating
  *
* */

/**
  * In the system, values, keys or both of them could be tracked
  * It also accepts optional rules to add tags
  * For example, people could add tags to only first or second key
  * of a key-value tuple
  *
* */

object TrackingType extends ConfEnumeration {
  type TrackingType = Value
  val Values = ConfValue("value")
  val Key = ConfValue("key")
  val KeyValues = ConfValue("key-value")
  val KeyValuesArray = ConfValue("key-value-array")
}

object TrackingTaint extends ConfEnumeration {
  type TrackingTaint = Value
  val IntTaint = ConfValue("int")
  val ObjTaint = ConfValue("obj")
  val SelectiveIntTaint = ConfValue("int-selective")
  val SelectiveObjTaint = ConfValue("obj-selective")
  val ObjImplicitTaint = ConfValue("obj-implicit")
}

object ShuffleOpt extends ConfEnumeration {
  type ShuffleOpt = Value
  val CombinedTag = ConfValue("ctag")
  val CacheTag = ConfValue("cache")
  val CCTag = ConfValue("cctag")
  val WithoutOpt = ConfValue("no-opt")
}

trait TapConf {
  val tap_input_before: Option[((InterruptibleIterator[(Any, Any)], String) => Iterator[(Any, Any)])] = None
  val tap_op_before: Option[((Partition, TaskContext, Iterator[Any], RDD[_]) => Iterator[Any])] = None /* run this func before each rdd func */
  val tap_op_after: Option[((Partition, TaskContext, Iterator[Any], RDD[_]) => Iterator[Any])] = None
  val tap_shuffle_before: Option[((Partition, TaskContext, Iterator[Any], RDD[_]) => Iterator[Any])] = None
  val tap_shuffle_after: Option[((Partition, TaskContext, Iterator[Any], RDD[_]) => Iterator[Any])] = None
  val tap_collect_after: Option[(Any => Any)] = None
  val tap_task_after: Option[((Int, Int) => Unit)] = None
  val tap_exception: Option[PartialFunction[Exception, Unit]] = None
}

class TrackingPolicy(val propagation_across_machines: Boolean,
                     val checkerConf: CheckerConf,
                     val tapConf: TapConf,
                     val tracking_type: TrackingType,
                     val objectTainter: Option[PartialFunction[Object, List[String]]] = None) extends Serializable {
  def this() =
    this (true, null, null, TrackingType.KeyValues)
}