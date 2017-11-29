package edu.hku.cs.dft.debug

/**
  * Created by jianyu on 4/13/17.
  */


// store the latest info of fail task
class DebugStorage(val stageId: Int, val partitionId: Int) {

  private var latest: Any = _

  private var func: _ => _ = _

  def push[T, U](o: T, f: T => U): Unit = {
    latest = o
    func = f
  }

  def pop(): (Any, _ => _) = (latest, func)

}

object DebugStorage {



}
