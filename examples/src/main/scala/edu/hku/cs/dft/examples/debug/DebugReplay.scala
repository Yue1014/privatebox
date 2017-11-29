package edu.hku.cs.dft.examples.debug

import java.io.{File, FileInputStream, IOException, ObjectInputStream}

/**
  * Created by jianyu on 4/25/17.
  */
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