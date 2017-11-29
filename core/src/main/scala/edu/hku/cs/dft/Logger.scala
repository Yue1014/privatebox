package edu.hku.cs.dft

/**
  * Created by max on 12/3/2017.
  */


// do not use logger for spark
trait Logger {
  def logInfo(a: Any)
}

object SparkLogger {
  def logInfo(o: Any): Unit = {
    println(o)
  }
}
