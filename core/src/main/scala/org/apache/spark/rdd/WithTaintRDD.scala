package org.apache.spark.rdd

import edu.hku.cs.dft.tracker.{CombinedTaint, SelectiveTainter, TupleTainter}
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
  * A [[WithTaintRDD]] has the data along with tag in corresponding position
  * But the problem is how to design the tag ???
  */

class WithTaintRDD[T: ClassTag](
  val prev: RDD[T]) extends RDD[(T, Any)](prev){
  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to compute a given partition.
    */
  override def compute(split: Partition, context: TaskContext): Iterator[(T, Any)] = {
    firstParent[T].iterator(split, context).map(d => {
      (d, TupleTainter.getTaint(d))
    })
  }

  /**
    * Implemented by subclasses to return the set of partitions in this RDD. This method will only
    * be called once, so it is safe to implement a time-consuming computation in it.
    *
    * The partitions in this array must satisfy the following property:
    * `rdd.partitions.zipWithIndex.forall { case (partition, index) => partition.index == index }`
    */
  override protected def getPartitions: Array[Partition] = this.firstParent.partitions
}
