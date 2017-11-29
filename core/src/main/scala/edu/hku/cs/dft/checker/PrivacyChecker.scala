package edu.hku.cs.dft.checker

import java.io.{BufferedReader, File, FileReader}

import edu.hku.cs.dft.network.Message
import edu.hku.cs.dft.tracker.TrackingTaint.TrackingTaint
import edu.hku.cs.dft.tracker._
import org.apache.spark.InterruptibleIterator

/**
  * Created by jianyu on 9/9/17.
  */
class PrivacyChecker extends IFTChecker {
  override val taint: TrackingTaint = TrackingTaint.IntTaint
  override val tapConf: TapConf = new TapConf {
    def parse(confFile: String): (String, Vector[Int]) = {
      val cfReadfer = new BufferedReader(new FileReader(new File(confFile)))
      var loop = true
      var seperator: String = ""
      var levels: Vector[Int] = Vector()
      while (loop) {
        val line = cfReadfer.readLine()
        if (line == null) loop = false
        if (loop && !line.startsWith("#")) {
          val keyValue = line.split("=")
          val key = keyValue(0).trim()
          val value = keyValue(1).trim()
          key match {
            case "Delimiter" => seperator = value
            case "SecurityLevel" => levels = value.split(",").map(_.toInt).toVector
          }
        }
      }
      if (seperator.equals("") || levels.isEmpty) {
        throw new Exception("file parsing error")
      }
      (seperator, levels)
    }

    override val tap_input_before: Option[(InterruptibleIterator[(Any, Any)], String) => Iterator[(Any, Any)]] = Some(
      (it: InterruptibleIterator[(Any, Any)], file: String) => {
        val (seperator, levels) = parse(file)
        val selectiveTainter: SelectiveTainter = new SelectiveTainter(Map(), 0)
        it.map(t => {
          val tainted = t._2 match {
            case s: String =>
              val splitString = s.split(seperator)
              val stringBuilder = new StringBuilder
              for (ss: (String, Int) <- splitString.zipWithIndex) {
                selectiveTainter.setFilter(Map[Int, Any => Int](0 -> ((_: Any) => levels(ss._2))))
                val taintedString = selectiveTainter.setTaint(ss._1)
                stringBuilder.append(taintedString)
                stringBuilder.append(seperator)
              }
            case _ => throw new IllegalArgumentException("only string is supported")
          }
          (t._1, tainted)
        })
      }
    )

  }
  override val localChecker: LocalChecker = new LocalChecker {

    case class IllegalAccess(tag: Int) extends Message

    override val checkFuncInt: Option[(Int) => Unit] = Some((tag: Int) => {
      if (tag != 0) {
        this.send(IllegalAccess(tag))
        throw new IllegalAccessError("contains tags")
      }
    })

    override def receiveAndReply(message: Message): Message = {
      null
    }

    override def onRegister(): Unit = {}

    override val id: String = "privacy"
  }
  override val globalChecker: GlobalChecker = new GlobalChecker {

    override def stop(): Unit = {}

    override def receiveAndReply(message: Message): Message = {
      null
    }

    override def onRegister(): Unit = {}

    override val id: String = "privacy"
  }
  override val across_machine: Boolean = true
}
