package edu.hku.cs.dft.tracker

import java.io.{BufferedReader, File, FileReader}

import edu.hku.cs.dft.tracker.Separator.Separator

/**
  * Created by max on 10/4/2017.
  */

/**
  * A auto tainter add tag to input automatically, according to
  * user rule
*/

trait AutoTainter {

  // read configuration file
  def initEngine(): Unit

  def setTaint[T](t: T): T

}

class SeEnumeration extends Enumeration {
  class SeVal(val string: String, val v: String) extends Val(string)
  def SeValue(string: String, v: String): SeVal = new SeVal(string, v)
}

object Separator extends SeEnumeration {
  type Separator = Value
  val Comma = SeValue("comma", ",")
  val Space = SeValue("space", " ")
  val Semicolon = SeValue("semicolon", ";")
  val Colon = SeValue("colon", ":")
}

class FullAutoTainter extends AutoTainter {

  private val autoTaint = 1 << 0

  private val selectiveTainter: SelectiveTainter =
    new SelectiveTainter(Map(), autoTaint)

  override def initEngine(): Unit = {}

  override def setTaint[T](t: T):T  = {
    selectiveTainter.setTaint(t)
  }

}

class TextAutoTainter(confFile: String) extends AutoTainter {

  /**
    * conf file format
    * separator = regularExpression
    * columns = user,name,gender(separatedd by comma)
    * taints = 1, 2, 3, 4(seperated by comma)
  */

  var separator: Separator = _

  var columns: List[String] = _

  var taints: Vector[Int] = Vector()

  val selectiveTainter: SelectiveTainter = new SelectiveTainter(Map(), 0)

  initEngine()

  override def initEngine(): Unit = {
    val cfReadfer = new BufferedReader(new FileReader(new File(confFile)))
    var loop = true
    while (loop) {
      val line = cfReadfer.readLine()
      if (line == null) loop = false
      if (loop && !line.startsWith("#")) {
        val keyValue = line.split("=")
        val key = keyValue(0).trim()
        val value = keyValue(1).trim()
        key match {
          case "separator" => separator = Separator.withName(value)
          case "columns" =>
            columns = value.split(",").toList
          case "taints" => taints = value.split(",")
            .map(t => t.trim.toInt).toVector
          case _ =>
        }
      }
    }

    taints.foreach(t => {
      println("add taint " + t)
    })
  }

  override def setTaint[T](t: T): T = {
    t match {
      case s: String =>
        val separatorVal = separator.asInstanceOf[Separator.SeVal].v
        val splitString = s.split(separatorVal)
        val stringBuilder = new StringBuilder
        for(ss: (String, Int) <- splitString.zipWithIndex) {
          selectiveTainter.setFilter(Map[Int, Any => Int](0 -> ((_: Any) => taints(ss._2))))
          val taintedString = selectiveTainter.setTaint(ss._1)
          stringBuilder.append(taintedString)
          stringBuilder.append(separatorVal)
        }
        stringBuilder.toString().asInstanceOf[T]
      case _ => throw new IllegalArgumentException("only string is supported")
    }
  }

}

object TextAutoTainter {

  val DefaultConfSuffix = ".tag"

}