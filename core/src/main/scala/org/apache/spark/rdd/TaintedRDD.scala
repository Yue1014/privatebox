package org.apache.spark.rdd

import edu.hku.cs.dft.tracker.{SelectiveTainter, TupleTainter}
import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
  * Created by jianyu on 4/21/17.
  */
class TaintedRDD[T: ClassTag](
  val prev: RDD[T], f: T => Any) extends RDD[T](prev){
  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to compute a given partition.
    */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    firstParent[T].iterator(split, context).map(d => {
      TupleTainter.setTaint(d, f(d))
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
