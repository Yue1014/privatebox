package edu.hku.cs.dft.examples

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

/**
  * Created by jianyu on 4/25/17.
  */
object GraphConnectionComputation {

  class JoinPartitioner(partitions: Int) extends HashPartitioner(partitions) {
    override def getPartition(key: Any): Int = {
      key match {
        case (_1, _2) => Math.abs(_1.hashCode()) % partitions
        case _ => Math.abs(key.hashCode()) % partitions
      }
    }
  }

    def main(args: Array[String]): Unit = {

      val spark = SparkSession
        .builder()
        .appName("Connection Computation")
        .getOrCreate()

      val file = if (args.length > 0) args(0) else throw new IllegalArgumentException("no input file")

      val numPartitions = if (args.length > 1) args(1).toInt else 8

      val partiallyPartition = if (args.length > 2) args(2).toBoolean else false

      val partiallyPartitioner = if (partiallyPartition)
        new JoinPartitioner(numPartitions)
      else
        new HashPartitioner(numPartitions)

      val text = spark.read.textFile(file).rdd
      val edges = text.map(t => {
        val s_arr = t.split("\\s+")
        (s_arr(0).toInt, s_arr(1).toInt)
      }).map(t => (t, null))
        .reduceByKey(partiallyPartitioner, (x, y) => x)
        .map(_._1)

      val nodes = edges.flatMap(edge => Array(edge._1, edge._2))

      val joinPartitioner = new JoinPartitioner(numPartitions)

      // Init the node with its own id
      var label_node = nodes.map(t => (t, t))

//      label_node.partitionBy(joinPartitioner)

      for (i <- 1 to 10) {
        // group nodes with the same id
        val new_label = label_node.join(edges).map(t => (t._2._2, t._2._1)).union(label_node)
        label_node = new_label.reduceByKey((_1, _2) => Math.min(_1, _2), numPartitions)
      }
      label_node.collect().foreach(println)

      readLine()
      spark.stop()

    }
  }
