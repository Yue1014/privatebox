package edu.hku.cs.dft

import edu.hku.cs.dft.dp.StandardLapNoiseGenerator

object LapNoiseGeneratorSuite {
  def main(args: Array[String]): Unit = {
    val g = new StandardLapNoiseGenerator(1)
    println(g.generate())
  }
}
