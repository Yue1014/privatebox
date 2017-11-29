package edu.hku.cs.dft.network

import java.io.ObjectOutputStream
import java.net.{InetAddress, Socket}

import edu.hku.cs.dft.{CheckerConf, DFTEnv}
import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.serialization.{ClassResolvers, ObjectDecoder, ObjectEncoder}

/**
  * Created by jianyu on 3/8/17.
  */
class NettyClient(endpointDispatcher: EndpointDispatcher, globalCheckerConf: CheckerConf) extends EndpointRegister {

  private var nettyHandle: NettyHandle = _

  private var eventLoopGroup: NioEventLoopGroup = _

  override def stop(): Unit = eventLoopGroup.shutdownGracefully()

  override def register(endPoint: EndPoint): Unit = {
    endPoint.asInstanceOf[NettyEndpoint].setHandle(nettyHandle)
    endpointDispatcher.registerEndpoint(endPoint)
    super.register(endPoint)
  }

  def run(): Unit = {
    eventLoopGroup = new NioEventLoopGroup()
    try {
      val boostrap = new Bootstrap()
      boostrap.group(eventLoopGroup)
        .channel(classOf[NioSocketChannel])
        .handler(new ChannelInitializer[SocketChannel] {
          override def initChannel(c: SocketChannel) {
            c.pipeline().addLast(new ObjectEncoder)
            c.pipeline().addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)))
            nettyHandle = new NettyClientHandler(endpointDispatcher.onReceive)
            c.pipeline().addLast(nettyHandle.asInstanceOf[NettyClientHandler])
          }
        })

      val f = boostrap.connect(globalCheckerConf.host, globalCheckerConf.port).sync()
      f.channel().closeFuture().sync()
    } finally {
      eventLoopGroup.shutdownGracefully()
    }
  }

}

class RuleEndpoint extends NettyEndpoint {
  val id: String = "Rule"

  def receiveAndReply(message: Message): Message = {
    println(message)
    message match {
      case _: onStart => onStarted(1)
      case _: onStarted => onEnded(1)
      case _ => null
    }
  }

  override def onRegister(): Unit = {

  }
}

class TestClass extends Serializable {
  private val r = 1
  private val m = 2
  val a: Array[Int] = new Array[Int](3)
  def add(): Int = r + m
  class InClass extends Serializable {
    val arr: Array[_] = new Array[Int](3)
  }
  val inClass: InClass = new InClass
}

object NettyClient{
  def main(args: Array[String]): Unit = {
    DFTEnv.executor(null)
    val nettyClient = new NettyClient(new EndpointDispatcher, null)
    new Thread(new Runnable {
      override def run() {
        nettyClient.run()
      }
    }).start()
    Thread.sleep(1000)

//    val client = new Socket(InetAddress.getByName("localhost"), 9999)
//    val out = new ObjectOutputStream(client.getOutputStream)
//    out.writeObject(DebugInfo(1, 2, 3))

    val rule = new RuleEndpoint
    nettyClient.register(rule)
    rule.send(DebugInformation(1, 2, null))
    rule.send(DebugInformation(1, 2, null))
  }
}