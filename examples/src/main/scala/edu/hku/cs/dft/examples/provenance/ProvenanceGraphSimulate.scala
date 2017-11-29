package edu.hku.cs.dft.examples.provenance
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by jianyu on 5/11/17.
  */
object ProvenanceGraphSimulate {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    val file = args(0)

    val partitions = args(1).toInt

    val iteration = args(2).toInt

    val trace_record = args(3)

    val sc = new SparkContext(conf)

    val text = sc.textFile(file, partitions)
    println(text)
    var edges = text.map(t => {
      val s_arr = t.split("\\s+")
      (s_arr(0), s_arr(1))
    })
    val nodes = edges.flatMap(edge => Array(edge._1, edge._2)).distinct()

    var label_node = nodes.zipWithIndex().map{
      case(node, id) =>
        (node, (id, Set(node)))
    }

    for (i <- 1 to iteration) {
      val new_label = label_node.join(edges).map{case (from, ((id, lineages), to)) =>
        (to, (id, lineages))
      }
      val m_label = new_label.union(label_node)
      label_node = m_label.reduceByKey((x, y) => {
        (math.min(x._1, y._1), x._2 | y._2)
      }, numPartitions = partitions)
    }

    label_node.filter(t => t._1.equals(trace_record)).collect().foreach(x => println(x._1 + " " + x._2._2.size))
    readLine()
    sc.stop()
  }
}
