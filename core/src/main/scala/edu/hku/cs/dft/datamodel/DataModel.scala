package edu.hku.cs.dft.datamodel

import edu.hku.cs.dft.datamodel.DataOperation.DataOperation
import edu.hku.cs.dft.optimization.RuleCollector
import edu.hku.cs.dft.optimization.RuleCollector.RuleSet

/**
  * Created by jianyu on 3/5/17.
  */

object DataOperation extends Enumeration {
  type DataOperation = Value
  val Map, Reduce, Union, Sample, ZipWithIndex, CoGroup, Input, None = Value
}

case class SerializableDataModel(ID: Int)

class DataModel(frameworkHandle: PlatformHandle) {

  private var _name: String = ""

  var count: Int = 0

  var ID: Long = 0

  var isOrigin = false

  def setName(sname: String): Unit = {
    _name = sname
  }

  def deps(): Map[DataModel, RuleSet] = _deps

  def handle(): PlatformHandle = frameworkHandle

  def name(): String = frameworkHandle.variable()

  def op(): DataOperation = frameworkHandle.op()

  private var _fathers: List[DataModel] = List()

  private var _sons: List[DataModel] = List()

  private var _deps: Map[DataModel, RuleSet] = Map()

  private var _dataType: Any = _

  def set_type(string: Any): Unit = _dataType = string

  def dataType(): Any = _dataType

  override def equals(obj: Any): Boolean = {
    obj match {
      case dataModel: DataModel => dataModel.ID == this.ID
      case _ => false
    }
  }

  def sons(): List[DataModel] = _sons

  def fathers(): List[DataModel] = _fathers

  def addDeps(ruleSet: RuleSet, dataModel: DataModel): Unit = {
    if (_deps == null)
      _deps = Map()
    val found = _deps.find(_._1 == dataModel)
    val returnDeps = if (found.isEmpty) {
      dataModel -> ruleSet
    } else {
      // Combine two RuleSet
      dataModel -> RuleCollector.CombineRule(ruleSet, found.get._2)
    }
    _deps += returnDeps
  }

  def addFathers(data: DataModel): Unit = {
    _fathers = data :: _fathers
  }

  def addSon(dataModel: DataModel): Unit = {
    _sons = dataModel :: _sons
  }

  /* Find the origin data model */
  def origin(): DataModel = {
    null
  }

  def frameworkId(): Int = frameworkHandle.frameworkId()

  override def toString: String = {
    val newBuilder = new StringBuilder
    newBuilder.append("[" + ID + "] ")
    newBuilder.append(" " + this.frameworkHandle.variable() + " ")
    newBuilder.append(" " + this.op + " ")
    newBuilder.append(frameworkHandle.frameworkName())
    newBuilder.append(" ")
    newBuilder.append(this._dataType)
    newBuilder.append(" => [ ")
    _sons.foreach(data => {
      newBuilder.append(data.ID)
      newBuilder.append(" ")
    })
    newBuilder.append("] ")
    if (_deps != null) {
      _deps.foreach(dep => {
        newBuilder.append(dep._1.ID)
        newBuilder.append(":")
        newBuilder.append(dep._2)
      })
    }
    newBuilder.append("\n")
    newBuilder.toString()
  }

  def serializableObject(): SerializableDataModel = SerializableDataModel(this.ID.toInt)

}
