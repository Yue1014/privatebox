package edu.hku.cs.dft.datamodel

import edu.hku.cs.dft.datamodel.DataOperation.DataOperation
import org.apache.spark.rdd.RDD
import scala.reflect.runtime.{universe => ru}
/**
  * Created by jianyu on 3/5/17.
  */

/**
 *  A [[PlatformHandle]] do ?
 *  To support a new system, things developers should do is to
 *  override PlatformHandle to support different platform and
 *  change the entry point in [[GraphManager]]
 */

trait PlatformHandle {
  def frameworkId(): Int
  def frameworkName(): String
  def fathers(): List[PlatformHandle]
  def frameworkType(): String
  def op(): DataOperation
  def variable(): String
}

class SparkPlatformHandle(frameworkR: RDD[_]) extends PlatformHandle {
  override def frameworkId(): Int = frameworkR.id

  override def frameworkName(): String = {
    frameworkR.name
  }

  override def frameworkType(): String = {
    frameworkR.rddType()
    ""
  }

  def getTypeTag[T: ru.TypeTag](obj: T): ru.TypeTag[T] = ru.typeTag[T]

  override def fathers(): List[PlatformHandle] = {
    var fa: List[PlatformHandle] = List()
    frameworkR.dependencies.foreach(dep => {
      fa = new SparkPlatformHandle(dep.rdd) :: fa
    })
    fa
  }

  override def op(): DataOperation = {
    frameworkR.rddType() match {
      case "HadoopRDD" => DataOperation.Input
      case "UnionRDD" => DataOperation.Union
      case "ShuffledRDD" => DataOperation.Reduce
      case "ZippedWithIndexRDD" => DataOperation.ZipWithIndex
      case "MapPartitionsRDD" => DataOperation.Map
      case "CoGroupedRDD" => DataOperation.CoGroup
      case _ => DataOperation.None
    }
  }

  override def variable():String = frameworkR.variableId
}