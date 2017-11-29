package edu.hku.cs.dft.debug

import java.io._

/**
  * Created by jianyu on 4/25/17.
  */
class DebugReplay {

  var failResult: List[(Any, Any, _ => _)] = List()

  def pushResult(record: Any, taint: Any, f: _ => _): Unit = {
    println("receive fail result")
    failResult = (record, taint, f) :: failResult
  }

  def writeToFile(file: String): Unit = {
    val oos = new ObjectOutputStream(new FileOutputStream(new File(file)))
    oos.writeObject(failResult)
    oos.close()
  }

}

object DebugReplay {
  val debugTraceFile: String = ".trace.debug"

  def main(args: Array[String]): Unit = {

    val f = new File(debugTraceFile)
    val failResult = try {
      new ObjectInputStream(new FileInputStream(f)).readObject()
    } catch {
      case e: IOException =>
        e.printStackTrace()
        System.exit(0)
    }
    failResult.asInstanceOf[List[(Any, Any, Any => _)]].foreach(record => {
      System.out.println("replay record " + record._1)
      val f = record._3
      f(record._1)
    })
  }

}
