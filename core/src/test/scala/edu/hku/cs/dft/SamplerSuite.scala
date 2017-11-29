package edu.hku.cs.dft

import edu.hku.cs.dft.optimization.Sampler


/**
  * Created by max on 15/3/2017.
  */
object SamplerSuite {
  def main(args: Array[String]): Unit = {
    val sampler = new Sampler(10)
    val arr = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)
    val filterResult = arr.filter(sampler.filterFunc)
    assert(filterResult.length == 20 / 10)
  }
}