/**
  * Created by jianyu on 3/18/17.
  */

package edu.hku.cs.tools

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class CallLocation(val location: String)

object CallLocation {
  implicit def capture: CallLocation = macro CallLocation.locationMacro

  def locationMacro(x: blackbox.Context): x.Expr[CallLocation] = {
    import x.universe._
    val position = x.enclosingPosition
    val file = position.source
    val line = position.line
    val column = position.column
    val where = s"$file:$line:$column"
    reify(new CallLocation(x.literal(where).splice))
  }

}