package edu.hku.cs.dft.network

import edu.hku.cs.dft.optimization.RuleCollector.RuleSet

/**
  * Created by jianyu on 3/8/17.
  */
trait EndPoint {
  val id: String
  // def onReceive(message: Message): Unit
  def receiveAndReply(message: Message): Message
  def send(obj: Message): Unit
  def onRegister()
}

trait EndpointRegister extends Runnable {
  def register(endPoint: EndPoint): Unit = {
    endPoint.onRegister()
  }
  def run(): Unit

  def stop(): Unit
}

trait NettyHandle {
  def sendMsg(obj: Any): Unit
}

abstract class NettyEndpoint extends EndPoint {

  private var nettyHandle: NettyHandle = _

  override def send(obj: Message): Unit = {
    nettyHandle.sendMsg(BoxMessage(this.id, obj))
  }

  def setHandle(_nettyHandle: NettyHandle): Unit = {
    nettyHandle = _nettyHandle
  }
}

trait Message

case class BoxMessage(endpointId: String, message: Message)

case class onStart(int: Int, string: String, double: Double) extends Message

case class onStarted(int: Int) extends Message

case class onEnded(int: Int) extends Message

case class EndpointError(string: String) extends Message

case class RuleRegister(bool: Boolean) extends Message

case class RuleRegistered(bool: Boolean) extends Message

case class RuleInfered(id: Int, depId: Int, ruleSet: RuleSet) extends Message

case class RuleAdded(id: Int) extends Message

case class DataType(id: Int, typeString: Any) extends Message

case class DataCount(id: Int, c: Int) extends Message

case class DebugInformation(record: Any, ta: Any, func: _ => _) extends Message