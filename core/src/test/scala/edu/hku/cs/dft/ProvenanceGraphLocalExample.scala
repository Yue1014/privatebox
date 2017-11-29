package edu.hku.cs.dft

import edu.hku.cs.dft.tracker.TupleTainter

/**
  * Created by jianyu on 4/21/17.
  */
object ProvenanceGraphLocalExample {

  case class Tracer(id: Int, index: Int)

  def main(args: Array[String]): Unit = {


    val edges = Array((1, 2), (1, 3), (1, 4), (2, 1), (3, 1), (4, 1))
    val nodes = edges.flatMap(edge => edge.productIterator).distinct.map(k => k.asInstanceOf[Int])

    var c = 0
    val tainted_edge = edges.map(s => {
      c = c + 1
      TupleTainter.setTaint(s, (Tracer(c, 1), Tracer(c, 2)))
    }
    )

    // Init the node with its own id
    var label_node: Map[Int, Int] = nodes.map(t => (t, t)).toMap

    for (i <- 1 to 10) {
      // group nodes with the same id
      val new_label = for {edge <- tainted_edge} yield (edge._2, label_node(edge._1))
      label_node = new_label.union(label_node.toList).groupBy(_._1).mapValues(t => {
        var min: Int = 0x99999
        t.foreach(m => {
          if (m._2 < min) {
            min = m._2
          }
        })
        min
      })
    }
    label_node.foreach(l => println(l + ":"+TupleTainter.getTaint(l._1) + " " + TupleTainter.getTaint(l._2)))
  }
}
