package edu.hku.cs.dft.tracker

import edu.hku.cs.dft.{CheckerConf, IftConf}
import edu.hku.cs.dft.tracker.TrackingTaint.TrackingTaint

/**
  * Created by max on 7/9/2017.
  */
trait IFTChecker {
  val taint: TrackingTaint
  val objectTainter: Option[PartialFunction[Object, List[String]]] = None
  val tapConf: TapConf
  val localChecker: LocalChecker
  val globalChecker: GlobalChecker
  val across_machine: Boolean
}

object IFTChecker {
  def setChecker(iftChecker: IFTChecker):IftConf = {
    val taps = iftChecker.tapConf
    val checkerConf = if (iftChecker.localChecker != null && iftChecker.globalChecker != null) {
      CheckerConf("127.0.0.1", 8790, iftChecker.localChecker, iftChecker.globalChecker)
    } else {
      null
    }
    val policy = new TrackingPolicy(iftChecker.across_machine, checkerConf, taps, TrackingType.KeyValues, iftChecker.objectTainter)
    IftConf(TrackingType.KeyValues, iftChecker.taint, ShuffleOpt.WithoutOpt, policy)
  }

  def defaultChecker(trackingTaint: TrackingTaint): IftConf = {
    val policy = new TrackingPolicy(true, null, null, TrackingType.KeyValues, None)
    IftConf(TrackingType.KeyValues, trackingTaint, ShuffleOpt.WithoutOpt, policy)
  }
}