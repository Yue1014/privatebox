package edu.hku.cs.dft.traffic

import org.apache.spark.HashPartitioner

/**
  * Created by jianyu on 3/24/17.
  */


// TODO: the hash in spark only deal with hashing of the key.. we may expand that to
// TODO: adjust our system

class DependentPartitioner(val partitions: Int, val hashKeySet:Set[Int]) extends HashPartitioner(partitions){

  override def equals(other: Any): Boolean = {
    other match {
      case d: DependentPartitioner => (partitions == d.partitions) && hashKeySet == hashKeySet
      case _ => false
    }
  }

  def keyAt(obj: Any, index: Int): (List[Any], Int) = {
    var valueSet: List[Any] = List()
    var currentIndex = index
    obj match {
      case p: Product => p.productIterator.foreach(t => {
        val s = keyAt(t, currentIndex)
        valueSet ++= s._1
        currentIndex = s._2
      })
        // add other iterable element here
      case _ =>
        if (hashKeySet.contains(currentIndex))
          valueSet = obj :: valueSet
        currentIndex += 1
    }
    (valueSet, currentIndex)
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  // use a naive and poor performing method first
  override def getPartition(key: Any): Int = {
    // construct the mapping index -> value
    nonNegativeMod(keyAt(key, 1)._1.hashCode(), numPartitions)
  }

}

/**
  * This [[OptimizedDependentPartitioner]] will take the type info of the data,
  * then it could use pattern matching to generate partition more efficiently
  * TODO leave for future work
*/
class OptimizedDependentPartitioner(val partitions: Int,
                                    val hashKeySet: Set[Int],
                                    val typeInfo: Any) extends HashPartitioner(partitions){

  override def getPartition(key: Any): Int = {
    0
  }

  override def equals(other: Any): Boolean = {
    other match {
      case d: OptimizedDependentPartitioner => (partitions == d.partitions) && hashKeySet == d.hashKeySet
      case _ => false
    }
  }

}