package edu.hku.cs.dft

import edu.hku.cs.dft.traffic.{PartitionScheme, PartitionSchemes}

/**
  * Created by jianyu on 3/24/17.
  */
object SchemesSerializationSuite {
  def main(args: Array[String]): Unit = {
//    var schemes: Map[String, PartitionScheme] = Map()
//    schemes += "a" -> PartitionScheme(1, Set(1, 2, 3), 1)
//    schemes += "b" -> PartitionScheme(2, Set(2, 3, 4), 2)
//    PartitionSchemes.serializeSchemes(scheme = schemes)
    val inputScheme = PartitionSchemes.parseSchemes("default.scheme")
    inputScheme.foreach(println)
//    schemes.foreach(t => {
//      assert(inputScheme(t._1) == t._2)
//    })
  }
}
