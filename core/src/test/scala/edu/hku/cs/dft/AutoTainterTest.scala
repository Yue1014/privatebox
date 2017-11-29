package edu.hku.cs.dft

import java.io.File

import edu.columbia.cs.psl.phosphor.runtime.Tainter
import edu.hku.cs.dft.tracker.{FullAutoTainter, TextAutoTainter}
import org.apache.hadoop.io.Text

/**
  * Created by jianyu on 4/10/17.
  */
object AutoTainterTest {

  def main(args: Array[String]): Unit = {
    val file = "/tmp/autoTainter.conf"

    val testString = "max 23 male"
    val autoTainter = if (new File(file).exists) {
      new TextAutoTainter(file)
    } else {
      new FullAutoTainter
    }
    autoTainter.initEngine()
    val taintedString = autoTainter.setTaint(testString)
    val name = taintedString.substring(0, 3)
    val age = taintedString.substring(4, 6)
    val gender = taintedString.substring(7, 9)
    val a = new Text
    a.set(age)
    val b = a.toString.toInt
    assert(Tainter.getTaint(age) == 2)
    assert(Tainter.getTaint(name) == 1)
    assert(Tainter.getTaint(gender) == 3)
    println(taintedString)
  }

}
