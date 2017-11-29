package edu.hku.cs.dft.network


/**
  * Created by jianyu on 3/8/17.
  */
class EndpointDispatcher {
  private var _endpointMap: Map[String, EndPoint] = Map()

  def onReceive(boxMessage: BoxMessage): BoxMessage = {
    val box = boxMessage.asInstanceOf[BoxMessage]
    val found = _endpointMap.find(_._1 == box.endpointId)
    /*
     * Ignore the message
    * */
    if (found.isEmpty) {
      println("could not find endpoint " + box.endpointId)
      return BoxMessage(box.endpointId, EndpointError("not found"))
    }
    val returnMsg = found.get._2.receiveAndReply(box.message)
    if (returnMsg == null) {
      null
    } else {
      BoxMessage(box.endpointId, returnMsg)
    }
  }

  def registerEndpoint(endPoint: EndPoint): Unit = {
    _endpointMap += endPoint.id -> endPoint
  }
}
