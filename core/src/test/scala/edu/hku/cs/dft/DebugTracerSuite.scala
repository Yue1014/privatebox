package edu.hku.cs.dft

import edu.hku.cs.dft.debug.DebugTracer

/**
  * Created by max on 13/4/2017.
  */

class DebugIterator[T](iterator: Iterator[T]) extends Iterator[T] {
  override def hasNext: Boolean = iterator.hasNext

  override def next(): T = {
    val nx = iterator.next()
    DebugTracer.trace(nx, null)
    nx
  }
}

object DebugTracerSuite {
  def main(args: Array[String]): Unit = {

/*    new Thread(new Runnable {
      override def run() = {
        DebugTracer.newStorage(3, 4)
        DebugTracer.trace(3)
        println(DebugTracer.backTrace() + " 3")
      }
    }).start()

    new Thread(new Runnable {
      override def run() = {
        DebugTracer.newStorage(3, 4)
        DebugTracer.trace(4)
        println(DebugTracer.backTrace() + " 4")
      }
    }).start()*/

    DebugTracer.newStorage(3, 4)
    try {
      val a = 1 to 100
      val g = a.map(t => t.toInt)
      val debugIterator = new DebugIterator(g.toIterator)
      debugIterator.map(println)
    } catch {
      case _: Exception => System.err.println(DebugTracer.backTrace())
    }

    System.out.println(DebugTracer.dumpObj(List(1,2,3,4).toIterator))

  }
}
