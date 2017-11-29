package edu.hku.cs.dft.dp

import scala.util.Random

  /**
    * [[LapNoiseGenerator]] generate random numbers with Laplace distribution
    * First, two random numbers (x and y) with uniform distribution should be generated
    * Then, generate the final number using the following equation:
    * Lap(x, b) = b * log ( x / y )
  */

trait LapNoiseGenerator {
  def generate(): Double
}

class NonSensitiveNoiseGenerator extends LapNoiseGenerator{
  override def generate(): Double = 0
}

class StandardLapNoiseGenerator(b: Double) extends LapNoiseGenerator{

  def generate(): Double = {
    val x = Random.nextDouble()
    val y = Random.nextDouble()
    if (y == 0)
      0
    else
      b * math.log(x / y)
  }

  def generate_fast(): Double = {
    val x = Random.nextDouble() - 0.5
    -b * sgn(x) * math.log(1 - 2 * math.abs(x))
  }

  def sgn(d: Double): Int = {
    if (d < 0)
      -1
    else if (d == 0)
      0
    else
      1
  }

}

object LapNoiseGenerator {
  val generator = new StandardLapNoiseGenerator(1)
  def generate(eps: Double, sensitivity: Double): Double = {
    generator.generate() * (sensitivity / eps)
  }
}