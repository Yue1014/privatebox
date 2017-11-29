package edu.hku.cs.dft.traffic

import edu.hku.cs.dft.DFTEnv
import edu.hku.cs.dft.optimization.Analyzer

/**
  * Created by max on 22/3/2017.
  */

/**
  * a [[PartitionSchemeTagger]] add partition scheme tag to a data model
  * a [[PartitionScheme]] consists of partition keySet(set of key index)
  * and the total number of keys that a data model have. A [[PartitionScheme]]
  * also have a r value to represent how strong this scheme will be
*/


case class PartitionScheme(keyCount: Int, hashKeySet: Set[Int], r: Int) {
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case PartitionScheme(_, hashKey, _) => hashKeySet.equals(hashKey)
      case s: Set[Int] => hashKeySet.equals(s)
      case _ => false
    }
  }
}

abstract class PartitionSchemeTagger extends Analyzer {

  // generated partition scheme
  var partitionTags: Map[String, Set[PartitionScheme]]

  var choosenSchemes: Map[String, PartitionScheme]

  def tagScheme(): Unit

  def chooseScheme(): Unit

  def printScheme(): Unit

  def serializeToFiles(fileNmae: String): Unit = PartitionSchemes.serializeSchemes(fileNmae
    ,choosenSchemes)
}