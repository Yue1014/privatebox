package edu.hku.cs.dft.optimization

import scala.util.Random

/**
  * Created by max on 15/3/2017.
  */

/**
  * A [[Sampler]] generate a filter function that do a uniform sampling of the original data
  * If fromZero is false, then the data will be sampled from a random position
  * Otherwise, data will be sampled from the first data
*/

class Sampler(sampleInt: Int, fromZero: Boolean = false) {
  private val samplePercent: Double = 100 / sampleInt
  private var sampleCurrent = Random.nextInt() % samplePercent
  def filterFunc[T](obj: T): Boolean = {
    val r = if (sampleCurrent % samplePercent == 0) {
      true
    } else {
      false
    }
    sampleCurrent += 1
    r
  }
}
