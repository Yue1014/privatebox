package edu.hku.cs.dft.checker

import edu.hku.cs.dft.tracker.TrackingTaint.TrackingTaint
import edu.hku.cs.dft.tracker._
import org.apache.spark.InterruptibleIterator

/**
  * Created by max on 23/9/2017.
  */
class DataProvananceChecker extends IFTChecker{
  override val taint: TrackingTaint = TrackingTaint.ObjTaint
  override val tapConf: TapConf = new TapConf {
    override val tap_input_before: Option[(InterruptibleIterator[(Any, Any)], String) => Iterator[(Any, Any)]] = Some(
      (iter: InterruptibleIterator[(Any, Any)], s: String) => {
      val c = iter.count(_ => true)
      iter.zip((1 to c).toIterator).map(t => {
        val t2 = TupleTainter.setTaint(t._1._2, t._2)
        (t._1._1, t2)
      })
    }
    )
    override val tap_collect_after: Option[(Any) => Any] = Some((t: Any) => {
      t match {
        case it: Iterable[_] => it.map(t => (t, TupleTainter.getTaint(t)))
        case o: Object => (o, TupleTainter.getTaint(o))
      }
    })
  }
  override val localChecker: LocalChecker = null
  override val globalChecker: GlobalChecker = null
  override val across_machine: Boolean = false
}
