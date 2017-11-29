package edu.hku.cs.dft

import edu.hku.cs.dft.traffic.DependentPartitioner

/**
  * Created by jianyu on 3/24/17.
  */
object DependentPartitionerTest {

  def main(args: Array[String]): Unit = {
    val p = new DependentPartitioner(3, Set(2, 1, 4))
    val m1 = ((1, 2, 3), 4)
    val m2 = ((1, 2, 1), 4)
    val m3 = ((2, 2, 3), 4)
    assert(p.getPartition(m1) == p.getPartition(m2))
    assert(p.getPartition(m2) != p.getPartition(m3))

    // test list
    val lp = new DependentPartitioner(3, Set(1))
    val lm1 = (List(1, 2, 3), (List(2, 2, 3), List(1, 2)))
    val lm2 = (List(1, 3, 2), (List(2, 2, 3), List(1, 2)))
    val lm3 = (List(2, 3, 2), (List(2, 2, 3), List(1, 2)))
    assert(lp.getPartition(lm1) == lp.getPartition(lm2))
    assert(lp.getPartition(lm2) != lp.getPartition(lm3))
  }

}
