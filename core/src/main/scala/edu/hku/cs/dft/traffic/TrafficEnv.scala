package edu.hku.cs.dft.traffic

/**
  * Created by jianyu on 3/26/17.
  */

class TrafficEnv(path: String) {
  // We may need to add some rule here

  val partitionSchemes: Map[String, PartitionScheme] =
    PartitionSchemes.parseSchemes(path)

  val partitionEnforcer: PartitionEnforcer =
    new PartitionEnforcer(partitionSchemes)
}

object TrafficEnv {
  val CONF_TRAFFIC: String = "spark.dft.traffic"
}