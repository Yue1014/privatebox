package edu.hku.cs.dft.tracker

import edu.hku.cs.dft.network.{Message, NettyEndpoint}

/**
  * Created by jianyu on 9/7/17.
  */
trait GlobalChecker extends NettyEndpoint with Serializable {
  def stop(): Unit
}
