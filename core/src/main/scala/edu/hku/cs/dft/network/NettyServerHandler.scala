package edu.hku.cs.dft.network

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandler, ChannelInboundHandlerAdapter, SimpleChannelInboundHandler}

/**
  * Created by jianyu on 3/8/17.
  */
class NettyServerHandler(f: BoxMessage => BoxMessage) extends ChannelInboundHandlerAdapter with NettyHandle{
  private val func = f
  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    val returnVal = func(msg.asInstanceOf[BoxMessage])
    if (returnVal != null) {
      ctx.writeAndFlush(returnVal)
    }
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close()
  }

  override def sendMsg(obj: Any) = throw new Exception("send to where")

}

class NettyClientHandler(f: BoxMessage => BoxMessage) extends ChannelInboundHandlerAdapter with NettyHandle {

  private var channelHandlerContext: ChannelHandlerContext = _

  private val func = f

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    channelHandlerContext = ctx
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any):Unit = {
    val returnMsg = func(msg.asInstanceOf[BoxMessage])
    if (returnMsg != null) {
      ctx.writeAndFlush(returnMsg)
    }
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }

  def sendMsg(obj: Any): Unit = {
    if (channelHandlerContext == null) throw new NullPointerException
    channelHandlerContext.writeAndFlush(obj)
  }
}