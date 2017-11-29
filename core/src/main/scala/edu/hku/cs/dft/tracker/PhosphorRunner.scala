package edu.hku.cs.dft.tracker

import java.io.File

import edu.hku.cs.dft.tracker.TrackingTaint.TrackingTaint


/**
  * Created by jianyu on 3/3/17.
  */
/**
  A [[PhosphorRunner]] build the environment to run the program in
 */

class PhosphorRunner(cacheDir: String, phospherJar: String, targetHome: String, var trackingTaint: TrackingTaint) {

  private def checkAndCreateCacheDir() {
    if (cacheDir != null) {
      val f = new File(cacheDir)
      if (!f.exists) {
        f.mkdir()
      }
    }
  }

  private val _agent = "-javaagent:" + phospherJar

  private def cache(): String = {
    checkAndCreateCacheDir()
    if (cacheDir != null) "=cacheDir=" + cacheDir + (
      trackingTaint match {
        case TrackingTaint.ObjTaint => "/obj"
        case TrackingTaint.IntTaint => "/int"
        case TrackingTaint.SelectiveIntTaint => "/int-selective"
        case TrackingTaint.SelectiveObjTaint => "/obj-selective"
        case _ => "/int"
      }) + "," else ""
  }
  private val _bootclasspath = "-Xbootclasspath/a:" + phospherJar

  private val _ignore = "checkTaint=true"

  private val _ignoreInt = "checkTaintIgnoreAll="

  def jreInst(): String = trackingTaint match {
    case TrackingTaint.IntTaint => "jre-inst-int"
    case TrackingTaint.ObjTaint => "jre-inst-obj"
    case TrackingTaint.SelectiveIntTaint => "jre-inst-int-selective"
    case TrackingTaint.SelectiveObjTaint => "jre-inst-obj-selective"
    case _ => "jre-inst-int"
  }

  def agent(checkTaint: Boolean = false, ignoreTaintAll: Int = 0): String = _agent + cache() +
    (if (checkTaint) _ignore + "," + _ignoreInt + ignoreTaintAll else "") + selective()

  def selective(): String = trackingTaint match {
    case TrackingTaint.SelectiveObjTaint => "withSelectiveInst=/tmp/a"
    case TrackingTaint.SelectiveIntTaint => "withSelectiveInst=/tmp/a"
    case _ => ""
  }


  def bootclasspath(): String = _bootclasspath

  def java(): String = targetHome + s"/${jreInst()}" + "/bin/java"

  def setTrackingTaint(taint: TrackingTaint): Unit = trackingTaint = taint

}
