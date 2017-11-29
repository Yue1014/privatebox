package edu.hku.cs.dft.network

import java.io.ObjectInputStream
import java.net.ServerSocket

import edu.hku.cs.dft.{CheckerConf, DFTEnv, TrackingMode}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.serialization.{ClassResolvers, ObjectDecoder, ObjectEncoder}

/**
  * Created by jianyu on 3/8/17.
  */
class NettyServer(endpointDispatcher: EndpointDispatcher, globalCheckerConf: CheckerConf) extends EndpointRegister {

  var nettyHandle: NettyHandle = _

  private var bossGroup: NioEventLoopGroup = _

  private var workerGroup: NioEventLoopGroup = _

  override def stop(): Unit = {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
  }

  override def register(endPoint: EndPoint): Unit = {
    endPoint.asInstanceOf[NettyEndpoint].setHandle(nettyHandle)
    endpointDispatcher.registerEndpoint(endPoint)
    super.register(endPoint)
  }

  def run() {
    // create it in new thread ?
    bossGroup = new NioEventLoopGroup()
    workerGroup = new NioEventLoopGroup()
    try {
      val bootstrapServer = new ServerBootstrap()
      bootstrapServer.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            ch.pipeline().addLast(new ObjectDecoder(ClassResolvers.cacheDisabled(null)))
            ch.pipeline().addLast(new ObjectEncoder)
            nettyHandle = new NettyServerHandler(endpointDispatcher.onReceive)
            ch.pipeline().addLast(nettyHandle.asInstanceOf[NettyServerHandler])
          }
        })

      val f = bootstrapServer.bind(globalCheckerConf.host, globalCheckerConf.port).sync()
      f.channel().closeFuture().sync()
    } finally {
      workerGroup.shutdownGracefully()
      bossGroup.shutdownGracefully()
    }
  }
}

object NettyServer {
  def main(args: Array[String]): Unit = {
    DFTEnv.server(null)
//    val nettyServer = new NettyServer(new EndpointDispatcher, DFTEnv.dftEnv())
//    new Thread(new Runnable {
//      override def run(): Unit = {
//        nettyServer.run()
//      }
//    }).start()

/*    val server = new ServerSocket(9999)
    while (true) {
      val s = server.accept()
      val inS = new ObjectInputStream(s.getInputStream)
      val in = inS.readObject()

      //      UninstTainter.fixStreamObject(in)
      in match {
        case t: TestClass => println(t.a.length)
        case a: Array[Int] => println(a.length)
        case _ => println("other")
      }
    }*/

//    nettyServer.register(new RuleEndpoint)
  }
}