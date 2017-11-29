package edu.hku.cs.dft.examples

/**
  * Created by jianyu on 3/21/17.
  */
import java.io.{File, FileInputStream, ObjectInputStream}

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.StdIn

class IndexHashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def getPartition(key: Any): Int = key match {
    case null => 0
    case two:(_,_) => {
      val index=(two._1,two._2)
      nonNegativeMod(index.hashCode, numPartitions)
    }
    case three:(_,_,_)=>{
      val index=(three._1,three._2)
      nonNegativeMod(index.hashCode, numPartitions)
    }
    case _ => 0
  }

  override def equals(other: Any): Boolean = other match {
    case h: IndexHashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}

object MatrixMultiplication{
  def main(args:Array[String]){
    if (args.length == 0){
      System.err.println("Usage: MatrixMultiplication <master> <MatrixA> <MatrixB> <k>")
      System.exit(1)
    }

    val inputA = args(0)
    val inputB = args(1)
    val k = args(2).toInt
//    val outputPath = args(3)
    val isPartitioned = args(3).toBoolean

    val conf = new SparkConf().setAppName("MatrixMultiplication")
    val sc = new SparkContext(conf)

    val arrA = sc.textFile(inputA, 40)
    val arrB = sc.textFile(inputB)

    val arrM = arrA.flatMap(entry => {
      val e = entry.split(" ")
      val i = e(0).toInt
      val j = e(1).toInt
      val v = e(2).toInt
      for {p <-1 until k+1} yield ((i,p,j),v)
    })

    val arrN = arrB.flatMap(entry => {
      val e = entry.split(" ")
      val i = e(0).toInt
      val j = e(1).toInt
      val v = e(2).toInt
      for {p <-1 until k+1} yield ((p,j,i),v)
    })

    val indexPartitioner = if(isPartitioned) {
      new IndexHashPartitioner(16)
    } else {
      new HashPartitioner(16)
    }

    val result = arrM.union(arrN).reduceByKey(indexPartitioner, (a,b) => a*b)
      .map(entry => {
        ((entry._1._1, entry._1._2), entry._2)
      })
      .reduceByKey(indexPartitioner, (a, b) => a + b)

    result.collect()
    StdIn.readLine()
    sc.stop()
  }
}