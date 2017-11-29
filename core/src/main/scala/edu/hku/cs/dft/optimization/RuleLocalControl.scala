package edu.hku.cs.dft.optimization

import edu.hku.cs.dft.network._
import edu.hku.cs.dft.network.NettyEndpoint
import edu.hku.cs.dft.tracker.DFTUtils

/**
  * Created by jianyu on 3/9/17.
  */

class TypeCollector(id: Int) {
  private var count = 0
  private var isSet = false
  var t: Any = 0
  def addType(obj: Any): Unit = {
    if (!isSet) {
      t = DFTUtils.getTypeTag(obj)
    }
    count += 1
  }
  def collect(): (Int, Any) = (count, t)
}

class SplitCollector(_split: String) {
  var ruleCollector: Map[Int, RuleCollector] = Map()
  var typeCollector: Map[Int, TypeCollector] = Map()
  val split: String = _split
  var origin: Int = 0

  def origin(id: Int): this.type = {
    origin = id
    this
  }

  def collectorInstance(_id: Int): RuleCollector = {
    val r = ruleCollector.getOrElse(_id, new RuleCollector(_id))
    ruleCollector += _id -> r
    r
  }

  def typeCollectorInstance(_id: Int): TypeCollector = {
    val t = typeCollector.getOrElse(_id, new TypeCollector(_id))
    typeCollector += _id -> t
    t
  }

}

//TODO Is this class atomic?
class RuleLocalControl extends NettyEndpoint{

  override val id: String = "Rule"

  override def receiveAndReply(message: Message): Message = {
    message match {
      case registered: RuleRegistered => null
      //TODO put the non-confirm rule to a pool and resent - need to do this?
      case added: RuleAdded => null
    }
  }

  override def onRegister(): Unit = {
    this.send(RuleRegister(true))
  }

  private var collector = 0

  // A (StageID + PartitionID) -> SplitCollector mapping
  private var splitCollectors: Map[String, SplitCollector] = Map()

  @deprecated
  private var typeCollectors: Map[Int, Any] = Map()

  @deprecated
  def addType(id: Int, string: Any): Unit = {
    typeCollectors += id -> string
  }

  def splitInstance(stage: Int, partition: Int): SplitCollector = {
    val id = stage + "-" + partition
    synchronized {
      val r = splitCollectors.getOrElse(id, new SplitCollector(id))
      splitCollectors += id -> r
      r
    }
  }

  def collect(stage: Int, partition: Int): Unit = {
    val _split = stage + "-" + partition
    val splitCollector = synchronized {
      val got = splitCollectors.getOrElse(_split, null)
      splitCollectors -= _split
      got
    }

    if (splitCollector != null) {
      splitCollector.ruleCollector.foreach(mm => {
        println("new " + _split + " " + mm._1 + " " + mm._2.collect())
        this.send(RuleInfered(mm._1, splitCollector.origin, mm._2.collect()))
      })
      splitCollector.typeCollector.foreach(mm => {
        val kv = mm._2.collect()
        this.send(DataCount(mm._1, kv._1))
        this.send(DataType(mm._1, kv._2))
      })
    }

    /*
    typeCollectors.foreach(t => {
      this.send(DataType(t._1, t._2))
    })
    */
  }

}
