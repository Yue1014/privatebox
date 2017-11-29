package edu.hku.cs.dft

import java.io.FileNotFoundException

import edu.hku.cs.dft.SampleMode.SampleMode
import edu.hku.cs.dft.TrackingMode.TrackingMode
import edu.hku.cs.dft.tracker.ShuffleOpt.ShuffleOpt
import edu.hku.cs.dft.tracker.TrackingTaint
import edu.hku.cs.dft.tracker.TrackingTaint.TrackingTaint
import edu.hku.cs.dft.tracker.TrackingType.TrackingType
import org.apache.spark.SparkConf

import scala.io.Source


/**
  * Created by max on 8/3/2017.
  */
/**
  * A [[ArgumentHandle]] specify the interface needed to parse the arguments for
  * our data-flow tracking framework
  * */

trait ArgumentHandle {
  def init(): Boolean
  def parseArgs(key: String): String
  def setKeyValue(key: String, value: String): Unit
}

/**
  * Parse configuration from spark
*/


/*
class SparkArgumentHandle(sparkConf: SparkConf) extends ArgumentHandle {
  override def init(): Boolean = true

  override def parseArgs(key: String): String = {
    sparkConf.get("spark.dft." + key)
  }
}
*/

class CommandlineHandle extends ArgumentHandle {

  private var storage: Map[String, String] = Map()

  override def init(): Boolean = true

  override def parseArgs(key: String): String = storage.getOrElse(key, null)

  override def setKeyValue(key: String, value: String): Unit = storage += key -> value

}

class ConfFileHandle(filename: String) extends ArgumentHandle {

  var keyMap: Map[String, String] = Map()

  try {
    println("Read configuration file from " + filename)
    for (line <- Source.fromFile(filename).getLines()) {
      if (!line.trim.startsWith("#")) {
        val arr = line.split("=")
        if (arr.length >= 3) throw new Exception("wrong format")
        val key = arr(0).trim
        val value = arr(1).trim
        keyMap += key -> value
        println("conf: " + key + " -> " + value)
      }
    }
  } catch {
    case e: FileNotFoundException => println("conf file " + filename + " not found")
    // use the default setting
  }

  override def init(): Boolean = true

  override def parseArgs(key: String): String = {
    keyMap.getOrElse(key, null)
  }

  override def setKeyValue(key: String, value: String): Unit = {
    keyMap += key -> value
  }
}

/**
* Parse the configuration from code
* */
class CustomArgumentHandle extends ArgumentHandle {
  override def init(): Boolean = true

  override def parseArgs(key: String): String = {
    key match {
      case DefaultArgument.CONF_HOST => DefaultArgument.host
      case DefaultArgument.CONF_PORT => DefaultArgument.port.toString
      case DefaultArgument.CONF_TRACKING => "mix"
      case DefaultArgument.CONF_SAMPLE => "off"
      case DefaultArgument.CONF_MODE => "server"
      case DefaultArgument.CONF_PHOSPHOR_JAVA => ""
      case DefaultArgument.CONF_PHOSPHOR_JAR => ""
      case DefaultArgument.CONF_PHOSPHOR_CACHE => "./phosphor_cache/"
      //TODO add partition
    }
  }

  override def setKeyValue(key: String, value: String) = {}
}

case class TrackingAppInfo(trackingTaint: TrackingTaint)

object DefaultArgument {

  val CONF_PREFIX = "--"

  val CONF_DUMP_PATH: String = "dft-dump-graph"
  val CONF_PARTITION_OUTPUT: String = "dft-partition-output"
  val CONF_HOST: String = "dft-host"
  val CONF_PORT: String = "dft-port"
  val CONF_TRACKING: String = "dft-tracking"
  val CONF_SAMPLE: String = "dft-sample"
  val CONF_MODE: String = "dft-mode"
  val CONF_TYPE: String = "dft-type"
  val CONF_PHOSPHOR_JAVA: String = "dft-phosphor-java"
  val CONF_PHOSPHOR_JAR: String = "dft-phosphor-jar"
  val CONF_PHOSPHOR_CACHE: String = "dft-phosphor-cache"
  val CONF_INPUT_TAINT: String = "dft-input-taint"
  val CONF_SAMPLE_INT: String = "dft-sample-int"
  val CONF_FILE: String = "dft-conf"
  val CONF_TAINT: String = "dft-taint"
  val CONF_SCHEME: String = "dft-scheme"
  val CONF_SHUFFLE: String = "dft-shuffle"

  val _CONF_HOST: String = CONF_PREFIX + CONF_HOST
  val _CONF_PORT: String = CONF_PREFIX + CONF_PORT
  val _CONF_TRACKING: String = CONF_PREFIX + CONF_TRACKING
  val _CONF_SAMPLE: String = CONF_PREFIX + CONF_SAMPLE
  val _CONF_MODE: String = CONF_PREFIX + CONF_MODE
  val _CONF_TYPE: String = CONF_PREFIX + CONF_TYPE
  val _CONF_INPUT_TAINT: String = CONF_PREFIX + CONF_INPUT_TAINT
  val _CONF_FILE: String = CONF_PREFIX + CONF_FILE
  val _CONF_TAINT: String = CONF_PREFIX + CONF_TAINT
  val _CONF_SHUFFLE: String = CONF_PREFIX + CONF_SHUFFLE

  val host: String = "127.0.0.1"
  val port: Int = 8787
  val trackingMode: TrackingMode = TrackingMode.Off
  val sampleMode: SampleMode = SampleMode.Off
  val mode: String = "server"
  // By default, 10% of the data is used when in the data sampling mode
  val sampleInt: Int = 10
  val partitionPath: String = "default.scheme"
  var confFile = "dft.conf"
  val dumpGraph = "graph.dump"
  val trackingTaint: TrackingTaint = TrackingTaint.IntTaint
}