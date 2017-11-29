package edu.hku.cs.dft.optimization

/**
  * Created by max on 18/3/2017.
  */
object SymbolManager {

  var scope: String = ""

  private var id: Int = 0
  private var StringId: Map[String, Int] = Map()

  def resetScope(s: String): Unit = {
    scope = s
    id = 0
  }

  def newInstance(): String = {
    id += 1
    scope + "-" + id
  }
}
