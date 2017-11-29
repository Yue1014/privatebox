package edu.hku.cs.dft.optimization

import edu.hku.cs.dft.checker.TrafficGlobal
import edu.hku.cs.dft.datamodel.DataOperation.DataOperation
import edu.hku.cs.dft.datamodel._
import edu.hku.cs.dft.optimization.RuleCollector.RuleSet
import edu.hku.cs.dft.tracker.DFTUtils

/**
  * Created by jianyu on 3/16/17.
  */
@SerialVersionUID(100L)
class LoopReducedDataModel(platformHandle: PlatformHandle, val variable: String) extends Serializable{

  var modelSet: Set[SerializableDataModel] = Set()

  var dataType: Any = _

  var count: Int = 0

  var dataCount: Int = 0

  private val _op = platformHandle.op()

  /**
    * A [[reduceKeyRange]] is to represent the reduce key set.
    * As a reduce operation needs [K, V] pair, so the (1 - reduceKey) will be the
    * Reduce Key Set, we only need to know the range
  */
  var reduceKeyRange: Int = 0

  var deps: Map[String, RuleSet] = Map()

  def op(): DataOperation = _op

  def addModel(dataModel: DataModel, fatherModel: DataModel):this.type = {
    modelSet += dataModel.serializableObject()
    count += 1
    dataCount += dataModel.count
    if (dataType == null)
      dataType = dataModel.dataType()

    // TODO: for debuging app, we do not need dependency, but the taint info
    if (fatherModel == null) {
      return this
    }
    dataModel.deps().foreach(dp => {
      val dpName = if (dp._1.name() == dataModel.name()) fatherModel.name() else dp._1.name()
      if (deps.contains(dpName)) {
        deps += dpName -> RuleCollector.CombineRule(dp._2, deps(dpName))
      } else {
        deps += dpName -> dp._2
      }
    })

    /**
      * fill the legacy deps with empty entry
    */

    dataModel.fathers().foreach(fa => {
      if (!deps.contains(fa.name())) {
        deps += fa.name() -> Map()
      }
    })
    this
  }

  override def toString: String = {
    val stringBuilder = new StringBuilder
    stringBuilder.append(s"[$variable] $dataType($dataCount) -> { ")

    // Print children id
    modelSet.foreach(d => {
      stringBuilder.append(d.ID)
      stringBuilder.append(" ")
    })
    stringBuilder.append(s"}($count) $op deps: ")

    // Print dependency
    deps.foreach(r => {
      stringBuilder.append(r._1 + " -> ")
      stringBuilder.append(r._2)
      stringBuilder.append(" ;")
    })
    stringBuilder.toString()
  }
}


/**
  * A [[Analyzer]] is used to all the info in the graph (variableID, depedentKeySet
  * and some other info that infers from the other semantics)
  *
  * TODO: Currently, the analyzer is in the App Driver, We may want to move this part
  * TODO: to a separated model, like saving the model into the disk
*/


class Analyzer extends Serializable{

  private val prefixRoot: String = "Root-"

  private var rootCurrent: Int = 1

  // String to String generation
  protected var setMap: Map[String, Set[String]] = Map()

  // String to set of dataModel, they are the same
  protected var dataSet: Map[String, LoopReducedDataModel] = Map()

  protected var rootData: List[String] = List()

  // set of data model that will cause shuffle of data
  // TODO: We should move this data and the calculation of this data to the subclasses
  protected var shuffleSet: Set[String] = Set()

  private var nullNum = 1

  private var visitedSet: Set[Int] = Set()

  def entry(graphManager: TrafficGlobal): Unit = {
    val dumpList = graphManager.rootData
    dumpList.foreach(dump => {
      if (!DFTUtils.nameValid(dump.name())) {
        dump.setName(prefixRoot + rootCurrent)
        rootCurrent += 1
      }
      rootData = dump.name() :: rootData
      entryHelper(dump, null)
    })
  }

  def entryHelper(dataModel: DataModel, fatherModel: DataModel): Unit = {
    // if the relation between this two datamodel name is missing,
    // then create the relation
    // if the relation exists,
    // then check if they have the same kind of child, and chang its current
    // name when they are different
    if (!DFTUtils.nameValid(dataModel.name())) {
      dataModel.setName("null" + nullNum)
      nullNum += 1
    }
    if (fatherModel != null) {
      if (setMap.contains(fatherModel.name())) {
        setMap += fatherModel.name() -> (setMap(fatherModel.name()) + dataModel.name())
      } else {
        setMap += fatherModel.name() -> Set[String](dataModel.name())
      }
    }
    if (visitedSet.contains(dataModel.ID.toInt))
      return

    dataSet += dataModel.name() -> dataSet.getOrElse(dataModel.name(), new LoopReducedDataModel(dataModel.handle(), dataModel.name())).addModel(dataModel, fatherModel)
    visitedSet += dataModel.ID.toInt
    dataModel.sons().foreach(t => {
      entryHelper(t, dataModel)
    })

  }

  /**
    * Before [[firstRoundEntry]], it only have the basic dep info
    * In the [[firstRoundEntry]], it will add the dependency info that may be hard or unnecessary
    * to infer
    * TODO: Now we only consider union, but we actually may use the information like union to provide
    * a higher-level information to optimize the system
    *
    * Also, it will add the reduce key range to each reduce ops
    *
    * We also need to find out the operations that may cause shuffle
    * For example, reduceByKey, union, cogroup,
    * TODO: Now we consider a null ops as map operation
    * if there are map operations that do not have deps, we should throw a Exception
    *
  */

  def firstRoundEntry(): Unit = {
    var checkList: List[String] = rootData
    var checkSet: Set[String] = Set()
    while(checkList.nonEmpty) {
      val v = checkList.last
      if (!checkSet.contains(v)) {

        // set the semantics dependencies and its data count
        dataSet(v).op() match {
/*          case DataOperation.Union =>
            dataSet(v).deps.foreach(kv => {
              dataSet(v).deps +=
                kv._1 -> Map(RuleMaker.makeOneToOneRuleFromTypeInfo(dataSet(v).dataType).toList -> dataSet(kv._1).dataCount)
            })
            // set data count
            var totalCount = 0
            dataSet(v).deps.foreach(kv => totalCount += dataSet(kv._1).dataCount)
            dataSet(v).dataCount = totalCount*/
            // todo: here the reduce is just reduce by key kind of reduce
            // todo: we may need to add a full dependency
          case DataOperation.CoGroup | DataOperation.Reduce =>
            dataSet(v).deps.foreach(kv => {
              dataSet(v).deps +=
                kv._1 -> Map(Map(1 -> List(1), 2 -> List(2)).toList -> dataSet(kv._1).dataCount)
            })
          case _ =>
          // TODO more rule
        }

        // get the reduce operations and set the reduce key set
        dataSet(v).op() match {
          case DataOperation.Reduce | DataOperation.CoGroup =>
            shuffleSet += v
            dataSet(v).reduceKeyRange = reduceKeyRange(dataSet(v).dataType)
          case DataOperation.None => // now we consider other operation as map
            // check if the deps exists
            var isDep = false
            dataSet(v).deps.foreach(ff =>
              if (ff._2.nonEmpty) {
                isDep = true
              }
            )
//            if (!isDep) throw new Exception("exists empty deps for null operations")
          case DataOperation.Map | DataOperation.Input | DataOperation.Union => // do nothing
          case _ => throw new Exception("invalid operation value")
        }

        if (setMap.contains(v)) {
          setMap(v).foreach(s => checkList = s :: checkList)
        }
      }
      checkSet += v
      checkList = checkList.init
    }

  }

  /**
    * If this is a key-value pair, then it will get the key range of this key-value pair
    * Or it will throw a Exception
  */
  def reduceKeyRange(kv: Any): Int = {
    kv match {
      case (k, _) => RuleMaker.typeInfoLength(k)
      case _ => throw new Exception("not a key value pair")
    }
  }

  /**
    * In the [[secondRoundEntry]], we will analyse the datamodel
    * from the root node, add partition tag to the node.
    * There may be multiple partition tag for a node,
    * so we will need a handler to choose partition tag
    *
  */

  def secondRoundEntry(): Unit = {

  }

  def dump(): Unit = {
    var startStrings: List[String] = rootData
    var dumpSet: Set[String] = Set()
    while (startStrings.nonEmpty) {
      val v = startStrings.last
      if (!dumpSet.contains(v)) {
        print(dataSet(v))
        dumpSet += v
        print(" ===>>> ")
        if (setMap.contains(v)) {
          setMap(v).foreach(k => {
            startStrings = k :: startStrings
            print(k + " ")
          })
        }
        println()
      }
      startStrings = startStrings.init
    }
  }

  // print the graph when exit
  def exitPoint(): Unit = {
    dump()
  }

}
