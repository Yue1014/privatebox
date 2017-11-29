package edu.hku.cs.dft

import edu.hku.cs.dft.datamodel.{GraphDumper, GraphManager}
import edu.hku.cs.dft.SampleMode.SampleMode
import edu.hku.cs.dft.TrackingMode.TrackingMode
import edu.hku.cs.dft.debug.DebugReplay
import edu.hku.cs.dft.network._
import edu.hku.cs.dft.optimization.RuleLocalControl
import edu.hku.cs.dft.tracker.ShuffleOpt.ShuffleOpt
import edu.hku.cs.dft.tracker.TrackingTaint.TrackingTaint
import edu.hku.cs.dft.tracker._
import edu.hku.cs.dft.tracker.TrackingType.TrackingType

/**
  * Created by jianyu on 3/6/17.
  */

class ConfEnumeration extends Enumeration {
  class ConfVal(val string: String) extends Val(string)
  def ConfValue(string: String): ConfVal = new ConfVal(string)
}


object TrackingMode extends ConfEnumeration {
  type TrackingMode = Value
  val RuleTracking = ConfValue("rule")
  val FullTracking = ConfValue("full")
  val SecurityTracking = ConfValue("security")
  val Debug = ConfValue("debug")
  val Off = ConfValue("off")
}

object SampleMode extends ConfEnumeration {
  type SampleMode = Value

  val Sample = ConfValue("sample")
  val Machine = ConfValue("machine")
  val Off = ConfValue("off")
}

case class CheckerConf(host: String, port: Int, localChecker: LocalChecker, globalChecker: GlobalChecker)

// sampleMode, sampleNum
// partition scheme

// Driver -> IftConf (policies) -> Executor (DFTEnv)

case class IftConf(trackingType: TrackingType,
                   trackingTaint: TrackingTaint, shuffleOpt: ShuffleOpt,
                   trackingPolicy: TrackingPolicy)

class DFTEnv {
  var isServer: Boolean = false
  var phosphorRunner: PhosphorRunner = _
  var trackingPolicy: TrackingPolicy = _
}

class DFTEnvOld(val argumentHandle: ArgumentHandle) {
  argumentHandle.init()

  val serverPort: Int = argumentHandle.parseArgs(DefaultArgument.CONF_PORT) match {
    case s: String => if (s.length > 0) s.toInt else DefaultArgument.port
    case _ => DefaultArgument.port
  }

  val serverHost: String = argumentHandle.parseArgs(DefaultArgument.CONF_HOST) match {
    case s: String => s
    case _ => DefaultArgument.host
  }

  var trackingMode: TrackingMode = {
    argumentHandle.parseArgs(DefaultArgument.CONF_TRACKING) match {
      case s: String => TrackingMode.withName(s)
      case _ => DefaultArgument.trackingMode
    }
  }

  val trackingOn: Boolean = {
    trackingMode match {
      case TrackingMode.Off => false
      case _ => true
    }
  }

  val trackingType: TrackingType = {
    argumentHandle.parseArgs(DefaultArgument.CONF_TYPE) match {
      case s: String => TrackingType.withName(s)
      case _ => TrackingType.KeyValues
    }
  }

  val sampleMode: SampleMode = {
    argumentHandle.parseArgs(DefaultArgument.CONF_SAMPLE) match {
      case s: String => SampleMode.withName(s)
      case _ => DefaultArgument.sampleMode
    }
  }

  //use percentage sampling
  val sampleNum: Int = {
    val stringInt = argumentHandle.parseArgs(DefaultArgument.CONF_SAMPLE_INT)
    if (stringInt != null) {
      Integer.valueOf(stringInt)
    } else {
      DefaultArgument.sampleInt
    }
  }

  var isServer: Boolean = argumentHandle.parseArgs(DefaultArgument.CONF_MODE) match {
    case "server" => true
    case "worker" => false
    case _ => false
  }

  val partitionSchemeOutput: String = {
    argumentHandle.parseArgs(DefaultArgument.CONF_PARTITION_OUTPUT) match {
      case s: String => s
      case _ => DefaultArgument.partitionPath
    }
  }

  val phosphorEnv: PhosphorEnv = {
    var java = argumentHandle.parseArgs(DefaultArgument.CONF_PHOSPHOR_JAVA)
    if (java == null) {
      java = "phosphor/bin/"
    }
    var jar = argumentHandle.parseArgs(DefaultArgument.CONF_PHOSPHOR_JAR)
    if (jar == null) {
      jar = "phosphor/phosphor.jar"
    }
    var cache = argumentHandle.parseArgs(DefaultArgument.CONF_PHOSPHOR_CACHE)
    if (cache == null) {
      cache = "phosphor/cache"
    }
    PhosphorEnv(java, jar, cache)
  }

  val graphDumpPath: String = argumentHandle.parseArgs(DefaultArgument.CONF_DUMP_PATH) match {
    case s: String => s
    case _ => DefaultArgument.dumpGraph
  }

  val trackingTaint: TrackingTaint = argumentHandle.parseArgs(DefaultArgument.CONF_TAINT) match {
    case s: String => TrackingTaint.withName(s)
    case _ => DefaultArgument.trackingTaint
  }
  
  val generateScheme: Boolean = argumentHandle.parseArgs(DefaultArgument.CONF_SCHEME) match {
    case "true" => true
    case _ => false
  }

  var shuffleOpt: ShuffleOpt = argumentHandle.parseArgs(DefaultArgument.CONF_SHUFFLE) match {
    case s: String => ShuffleOpt.withName(s)
    case _ => ShuffleOpt.WithoutOpt
  }

}

case class PhosphorEnv(phosphorJava: String, phosphorJar: String, cache: String)

object DFTEnv {
  var currentIft: DFTEnv = _

  var iftConf: IftConf = _

  var localChecker: LocalChecker = _

  var globalChecker: GlobalChecker = _

  var networkChannel: EndpointRegister = _

  var objectTainter: Option[PartialFunction[Object, List[String]]] = None

  var tap: Boolean = false

  var taps: TapConf = _

  def on(): Boolean = currentIft != null

  def ift(): DFTEnv = if (currentIft != null) currentIft else throw new Exception("DFT Environment not set")

  def conf(): IftConf  = if (iftConf != null) iftConf else throw new Exception("DFT Environment not set")

  def executor(iftConf: IftConf): Unit = {
    if (iftConf != null) {
      currentIft = new DFTEnv()
      currentIft.trackingPolicy = iftConf.trackingPolicy
      this.iftConf = iftConf
      objectTainter = iftConf.trackingPolicy.objectTainter
      if (iftConf.trackingPolicy.checkerConf != null) {
        localChecker = iftConf.trackingPolicy.checkerConf.localChecker
        networkChannel = new NettyClient(new EndpointDispatcher, iftConf.trackingPolicy.checkerConf)
        new Thread(networkChannel).start()
        val localEndPoint = new NettyEndpoint {

          override def receiveAndReply(message: Message): Message = localChecker.receiveAndReply(message)

          override def onRegister(): Unit = {}

          override val id: String = localChecker.id

        }
        networkChannel.register(localEndPoint)
      }
      taps = iftConf.trackingPolicy.tapConf
      if (taps != null)
        tap = true
    }
  }

  def server(iftConf: IftConf): Unit = {
    currentIft = new DFTEnv
    currentIft.isServer = true
    this.iftConf = iftConf
    if (iftConf != null && iftConf.trackingPolicy.checkerConf != null) {
      networkChannel = new NettyServer(new EndpointDispatcher, iftConf.trackingPolicy.checkerConf)
      new Thread(networkChannel).start()
      networkChannel.register(new NettyEndpoint {

        override def receiveAndReply(message: Message): Message = globalChecker.receiveAndReply(message)

        override def onRegister(): Unit = {}

        override val id: String = globalChecker.id
      })
    }
  }

  def worker(): Unit = {
    val argumentHandle = new ConfFileHandle(DefaultArgument.confFile)
    currentIft = new DFTEnv
    var java = argumentHandle.parseArgs(DefaultArgument.CONF_PHOSPHOR_JAVA)
    if (java == null) {
      java = "phosphor/bin/"
    }
    var jar = argumentHandle.parseArgs(DefaultArgument.CONF_PHOSPHOR_JAR)
    if (jar == null) {
      jar = "phosphor/phosphor.jar"
    }
    var cache = argumentHandle.parseArgs(DefaultArgument.CONF_PHOSPHOR_CACHE)
    if (cache == null) {
      cache = "phosphor/cache"
    }
    PhosphorEnv(java, jar, cache)
    currentIft.phosphorRunner = new PhosphorRunner(cache, jar, java, TrackingTaint.IntTaint)
  }

  def stop_all(): Boolean = {
    if (iftConf != null && networkChannel != null) {
      networkChannel.stop()
      if (currentIft.isServer) globalChecker.stop()
    }
    true
  }
}
