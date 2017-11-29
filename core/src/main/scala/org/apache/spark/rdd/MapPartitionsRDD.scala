/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd


import edu.hku.cs.dft.DFTEnv
import edu.hku.cs.dft.optimization.RuleCollector
import edu.hku.cs.dft.tracker.RuleTainter

import scala.reflect.ClassTag
import org.apache.spark.{Partition, TaskContext}

/**
 * An RDD that applies the provided function to every partition of the parent RDD.
 */
private[spark] class MapPartitionsRDD[U: ClassTag, T: ClassTag](
    var prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
    preservesPartitioning: Boolean = false)
  extends RDD[U](prev) {

  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    val prev = if (DFTEnv.tap && DFTEnv.taps.tap_op_before.isDefined)
      DFTEnv.taps.tap_op_before.get(split, context, firstParent[T].iterator(split, context), this).asInstanceOf[Iterator[T]]
    else
      firstParent[T].iterator(split, context)
    val proc = f(context, split.index, prev)
    if (DFTEnv.tap && DFTEnv.taps.tap_op_after.isDefined)
      DFTEnv.taps.tap_op_after.get(split, context, proc, this).asInstanceOf[Iterator[U]]
    else
      proc
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
