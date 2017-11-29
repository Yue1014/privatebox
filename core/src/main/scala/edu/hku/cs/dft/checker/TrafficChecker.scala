package edu.hku.cs.dft.checker

import edu.hku.cs.dft.DFTEnv
import edu.hku.cs.dft.datamodel.{DataModel, GraphDumper, PlatformHandle, SparkPlatformHandle}
import edu.hku.cs.dft.network._
import edu.hku.cs.dft.optimization.RuleCollector.RuleSet
import edu.hku.cs.dft.optimization.{RuleCollector, SplitCollector, TypeCollector}
import edu.hku.cs.dft.tracker.TrackingTaint.TrackingTaint
import edu.hku.cs.dft.tracker._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

/**
  * Created by jianyu on 9/8/17.
  */

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

object TrafficChecker {
  private var collector = 0

  // A (StageID + PartitionID) -> SplitCollector mapping
  private var splitCollectors: Map[String, SplitCollector] = Map()

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
        DFTEnv.localChecker.send(RuleInfered(mm._1, splitCollector.origin, mm._2.collect()))
      })
      splitCollector.typeCollector.foreach(mm => {
        val kv = mm._2.collect()
        DFTEnv.localChecker.send(DataCount(mm._1, kv._1))
        DFTEnv.localChecker.send(DataType(mm._1, kv._2))
      })
    }

    /*
    typeCollectors.foreach(t => {
      this.send(DataType(t._1, t._2))
    })
    */
  }
}

class TrafficGlobal extends GlobalChecker{

  val dumpPath = "graph.dump"

  override val id: String = "traffic"

  override def receiveAndReply(message: Message): Message = {
    message match {
      case register: RuleRegister => null
      case rule: RuleInfered =>
        addTrackingRule(rule.id, rule.depId, rule.ruleSet)
        null
      case tt: DataType =>
        addTypeData(tt.id, tt.typeString)
        null
      case c: DataCount =>
        addCount(c.id, c.c)
        null
      case _ => null
    }
  }

  var frameworkIdMapData: Map[Int, DataModel] = Map()

  var rootData: List[DataModel] = List()

  private var _currentId: Long = 0

  def currentId(): Long = {
    val returnId = _currentId
    _currentId += 1
    returnId
  }

  def spark_entry(rDD: RDD[_]) {
    entry(new SparkPlatformHandle(rDD))
  }

  /* trigger this function when one task is added */
  def entry(platformHandle: PlatformHandle): Unit = {
    getOrElseCreate(platformHandle)
  }

  def getOrElseCreate(platformHandle: PlatformHandle): DataModel = {
    val found = frameworkIdMapData.find(_._1 == platformHandle.frameworkId())
    val returnVal = if (found.isEmpty) {
      val data = new DataModel(platformHandle)
      val fathersHandle = platformHandle.fathers()
      if (fathersHandle.isEmpty) {
        data.isOrigin = true
        rootData = data :: rootData
      } else {
        fathersHandle.foreach(fatherHandle => {
          val father = getOrElseCreate(fatherHandle)
          father.addSon(data)
          data.addFathers(father)
        }
        )
      }
      data.ID = currentId()
      data
    } else {
      found.get._2
    }
    frameworkIdMapData += platformHandle.frameworkId() -> returnVal
    returnVal
  }

  def getDatamodelOrThrow(platformId: Int): DataModel = {
    val found = frameworkIdMapData.find(_._1 == platformId)
    if (found.isEmpty) {
      throw DataNotFoundException(platformId)
    }
    found.get._2
  }

  def addTrackingRule(platformId: Int, depDataId: Int,ruleSet: RuleSet): Unit = {
    if (ruleSet.nonEmpty) {
      val ruleData = getDatamodelOrThrow(platformId)
      val depData = getDatamodelOrThrow(depDataId)
      ruleData.addDeps(ruleSet, depData)
    }
  }

  def addTypeData(int: Int, string: Any): Unit = {
    getDatamodelOrThrow(int).set_type(string)
  }

  def addCount(int: Int, c: Int): Unit = {
    val data = getDatamodelOrThrow(int)
    data.count += c
  }

  case class DataNotFoundException(i: Int) extends Exception

  override def onRegister(): Unit = {}

  override def stop(): Unit = {
    val graphDumper = new GraphDumper(dumpPath)
    graphDumper.open()
    graphDumper.dumpGraph(DFTEnv.globalChecker.asInstanceOf[TrafficGlobal])
    graphDumper.close()
  }
}

class TrafficChecker extends IFTChecker {

  override val taint: TrackingTaint = TrackingTaint.IntTaint
  override val tapConf: TapConf = new TapConf {
    override val tap_shuffle_before: Option[(Partition, TaskContext, Iterator[Any], RDD[_]) => Iterator[Any]] = Some(
      (split: Partition, context: TaskContext, it: Iterator[Any], rdd: RDD[_]) => {
      val collector = TrafficChecker.splitInstance(context.stageId(), split.index).origin(rdd.id).collectorInstance(rdd.id)
      val tainter = new RuleTainter(DFTEnv.conf().trackingPolicy, collector)
      it.map(tainter.setTaint)
    })
    override val tap_shuffle_after: Option[(Partition, TaskContext, Iterator[Any], RDD[_]) => Iterator[Any]] = Some(
      (split: Partition, context: TaskContext, it: Iterator[Any], rdd: RDD[_]) => {
      val collector = TrafficChecker.splitInstance(context.stageId(), split.index).collectorInstance(rdd.id)
      val tainter = new RuleTainter(DFTEnv.conf().trackingPolicy, collector)
      it.map(tainter.getTaintAndReturn)
    })
    override val tap_task_after: Option[(Int, Int) => Unit] = Some((stageId: Int, parId: Int) => {
      TrafficChecker.collect(stageId, parId)
    })
  }
  override val localChecker: LocalChecker = new LocalChecker {

    override val id: String = "traffic"

    override def receiveAndReply(message: Message): Message = {
      null
    }

    override def onRegister(): Unit = {}
  }
  override val globalChecker: GlobalChecker = new TrafficGlobal

  override val across_machine: Boolean = false

}
