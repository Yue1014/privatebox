package edu.hku.cs.dft.dp

import breeze.linalg.DenseVector

trait DPModel {
  val MAX_LEVEL: Int = 8
  val levels = Array(10, 7.8, 5.2, 1)
  def get_eps_from_level(level: Int): Double = level match {
    case 0 => 0
    case 1 => levels(0)
    case 2 => levels(1)
    case 3 => levels(1)
    case 4 => levels(2)
    case 5 => levels(2)
    case 6 => levels(2)
    case 7 => levels(2)
    case 8 => levels(3)
    case _ => levels(3)
  }
  def getNoiseGenerator(level: Int, sensitivity: Double): LapNoiseGenerator = {
    level match {
      case 0 => new NonSensitiveNoiseGenerator
      case _ => new StandardLapNoiseGenerator(sensitivity / get_eps_from_level(level))
    }
  }
  def pre_average_op[T](iterable: Iterable[(T, Any)]): Iterable[(Any, Any)] = {
    iterable
  }
  def final_op[T](iterable: Iterable[(Any, Any)], in: scala.Vector[Array[(T, Any)]]): Iterable[Any] = iterable
}

object AnyOrder extends Ordering[Any] {
  override def compare(x: Any, y: Any) = {
    x match {
      case s: String =>
        s compare y.asInstanceOf[String]
      case i: Int =>
        i compare y.asInstanceOf[Int]
      case l: Long =>
        l compare y.asInstanceOf[Long]
      case d: Double =>
        d compare y.asInstanceOf[Double]
      case _ =>
        throw new IllegalArgumentException(x.toString)
    }
  }
}

// share a gloabal minimum eps
class GUPTDPModel(par: Int) extends DPModel{
  override def final_op[T](iterable: Iterable[(Any, Any)], in: scala.Vector[Array[(T, Any)]]): Iterable[Any] = {
    val max_min = in.map(t => {
      t.map(g => (g._1, g._1)).reduceLeft((x, y) => (DPAggregator.com_two(DPAggregator.com_max)(x._1, y._1).asInstanceOf[T],
        DPAggregator.com_two(DPAggregator.com_min)(x._2, y._2).asInstanceOf[T]))
    }).reduceLeft((x, y) => (DPAggregator.com_two(DPAggregator.com_max)(x._1, y._1).asInstanceOf[T],
      DPAggregator.com_two(DPAggregator.com_min)(x._2, y._2).asInstanceOf[T]))

    val max_minus_min = DPAggregator.com_two(DPAggregator.com_minus)(max_min._1, max_min._2)

    val generator = max_minus_min match {
      case p: Product =>
        p.productIterator.map(t => t match {
          case i: Int => getNoiseGenerator(MAX_LEVEL, i.toDouble / par)
          case d: Double => getNoiseGenerator(MAX_LEVEL, d.toDouble / par)
          case l: Long => getNoiseGenerator(MAX_LEVEL, l.toDouble / par)
          case s: Short => getNoiseGenerator(MAX_LEVEL, s.toDouble / par)
          case _ => 0
        }).toVector
      case i: Iterable[_] => i.map(t => t match {
        case i: Int => getNoiseGenerator(MAX_LEVEL, i.toDouble / par)
        case d: Double => getNoiseGenerator(MAX_LEVEL, d.toDouble / par)
        case l: Long => getNoiseGenerator(MAX_LEVEL, l.toDouble / par)
        case s: Short => getNoiseGenerator(MAX_LEVEL, s.toDouble / par)
        case _ => 0
      }).toVector
      case i: Int => getNoiseGenerator(MAX_LEVEL, i.toDouble / par)
      case d: Double => getNoiseGenerator(MAX_LEVEL, d.toDouble / par)
      case l: Long => getNoiseGenerator(MAX_LEVEL, l.toDouble / par)
      case s: Short => getNoiseGenerator(MAX_LEVEL, s.toDouble / par)
      case _ => 0
    }
    iterable.map(t => {
      val r = DPAggregator.com_noise_tuple(t._1, generator)
      val key = t._1 match {
        case p: Product => p.productElement(0)
        case i: Iterable[_] => i.head
        case i: DenseVector[Double] => i(0)
        case _ => t._1
      }
      (key, r)
    }).toVector.sortBy(_._1)(AnyOrder).map(_._2)
  }
}

// each column has its levels
class NaiveDPModel(par: Int) extends DPModel{

  override def final_op[T](iterable: Iterable[(Any, Any)], in: scala.Vector[Array[(T, Any)]]): Iterable[Any] = {
    val max_min = in.map(t => {
      t.map(g => (g._1, g._1)).reduceLeft((x, y) => (DPAggregator.com_two(DPAggregator.com_max)(x._1, y._1).asInstanceOf[T],
        DPAggregator.com_two(DPAggregator.com_min)(x._2, y._2).asInstanceOf[T]))
    }).reduceLeft((x, y) => (DPAggregator.com_two(DPAggregator.com_max)(x._1, y._1).asInstanceOf[T],
      DPAggregator.com_two(DPAggregator.com_min)(x._2, y._2).asInstanceOf[T]))

    val max_minus_min = DPAggregator.com_two(DPAggregator.com_minus)(max_min._1, max_min._2)

    // calculate the levels
    val levels = in.map(t => t.map(_._2).reduceLeft(DPAggregator.com_two(DPAggregator.com_max))).reduceLeft(DPAggregator.com_two(DPAggregator.com_max))
    val generator = max_minus_min match {
      case p: Product =>
        p.productIterator.zip(levels.asInstanceOf[Product].productIterator).map(t => t._1 match {
          case i: Int => getNoiseGenerator(t._2.asInstanceOf[Int], i.toDouble / par)
          case d: Double => getNoiseGenerator(t._2.asInstanceOf[Int], d.toDouble / par)
          case l: Long => getNoiseGenerator(t._2.asInstanceOf[Int], l.toDouble / par)
          case s: Short => getNoiseGenerator(t._2.asInstanceOf[Int], s.toDouble / par)
          case _ => 0
        }).toVector
      case i: Iterable[_] => i.zip(levels.asInstanceOf[Iterable[_]]).map(t => t._1 match {
        case i: Int => getNoiseGenerator(t._2.asInstanceOf[Int], i.toDouble)
        case d: Double => getNoiseGenerator(t._2.asInstanceOf[Int], d.toDouble)
        case l: Long => getNoiseGenerator(t._2.asInstanceOf[Int], l.toDouble)
        case s: Short => getNoiseGenerator(t._2.asInstanceOf[Int], s.toDouble)
        case _ => 0
      }).toVector
      case i: DenseVector[Double] => i.toArray.zip(levels.asInstanceOf[Iterable[_]]).map(t => t._1 match {
        case d: Double => getNoiseGenerator(t._2.asInstanceOf[Int], d.toDouble)
        case _ => 0
      }).toVector
      case i: Int => getNoiseGenerator(levels.asInstanceOf[Int], i.toDouble / par)
      case d: Double => getNoiseGenerator(levels.asInstanceOf[Int], d.toDouble / par)
      case l: Long => getNoiseGenerator(levels.asInstanceOf[Int], l.toDouble / par)
      case s: Short => getNoiseGenerator(levels.asInstanceOf[Int], s.toDouble / par)
      case _ => 0
    }
    iterable.map(t => {
      val key = t._1 match {
        case p: Product => p.productElement(0)
        case i: Iterable[_] => i.head
        case d: DenseVector[Double] => d(0)
        case _ => t._1
      }
      (key, DPAggregator.com_noise_tuple(t._1, generator))
    }).toVector.sortBy(_._1)(AnyOrder).map(_._2)
  }
}

// each partition has its levels
class CombinedDPModel extends DPModel {

  override def pre_average_op[T](iterable: Iterable[(T, Any)]): Iterable[(Any, Any)] = {
    val levels = iterable.map(_._2).reduceLeft(DPAggregator.com_two(DPAggregator.com_max))
    val min_max = iterable.map(_._1).map(t => (t, t)).reduceLeft((x, y) =>
      (DPAggregator.com_two(DPAggregator.com_max)(x._1, y._1).asInstanceOf[T],
       DPAggregator.com_two(DPAggregator.com_min)(x._2, y._2).asInstanceOf[T]))
    val delta_f = DPAggregator.com_two(DPAggregator.com_minus)(min_max._1, min_max._2)
    val generator = delta_f match {
      case p: Product =>
        p.productIterator.zip(levels.asInstanceOf[Product].productIterator).map(t => t._1 match {
          case i: Int => getNoiseGenerator(t._2.asInstanceOf[Int], i.toDouble)
          case d: Double => getNoiseGenerator(t._2.asInstanceOf[Int], d.toDouble)
          case l: Long => getNoiseGenerator(t._2.asInstanceOf[Int], l.toDouble)
          case s: Short => getNoiseGenerator(t._2.asInstanceOf[Int], s.toDouble)
          case _ => 0
        }).toVector
      case i: Iterable[_] => i.zip(levels.asInstanceOf[Iterable[_]]).map(t => t._1 match {
        case i: Int => getNoiseGenerator(t._2.asInstanceOf[Int], i.toDouble)
        case d: Double => getNoiseGenerator(t._2.asInstanceOf[Int], d.toDouble)
        case l: Long => getNoiseGenerator(t._2.asInstanceOf[Int], l.toDouble)
        case s: Short => getNoiseGenerator(t._2.asInstanceOf[Int], s.toDouble)
        case _ => 0
      }).toVector
      case i: DenseVector[Double] => i.toArray.zip(levels.asInstanceOf[Iterable[_]]).map(t => t._1 match {
        case d: Double => getNoiseGenerator(t._2.asInstanceOf[Int], d.toDouble)
        case _ => 0
      }).toVector
      case i: Int => getNoiseGenerator(levels.asInstanceOf[Int], i.toDouble)
      case d: Double => getNoiseGenerator(levels.asInstanceOf[Int], d.toDouble)
      case l: Long => getNoiseGenerator(levels.asInstanceOf[Int], l.toDouble)
      case s: Short => getNoiseGenerator(levels.asInstanceOf[Int], s.toDouble)
      case _ => 0
    }
    iterable.map(t => (DPAggregator.com_noise_tuple(t._1, generator), t._2))
  }

  override def final_op[T](iterable: Iterable[(Any, Any)], in: scala.Vector[Array[(T, Any)]]): Iterable[Any] = {
    iterable.map(_._1)
  }
}

class SplitDPModel(split: Int) extends DPModel {
  override def final_op[T](iterable: Iterable[(Any, Any)], in: scala.Vector[Array[(T, Any)]]): Iterable[Any] = {
    val min_max = iterable.map(t => (t._1, t._1)).
      reduceLeft((x, y) =>
        (DPAggregator.com_two(DPAggregator.com_max)(x._1, y._1),
          DPAggregator.com_two(DPAggregator.com_min)(x._2, y._2)))
    val delta = DPAggregator.com_two(DPAggregator.com_minus)(min_max._1, min_max._2)
    iterable.map(t => {
      val levels = t._2
      val z = delta match {
        case p: Product =>
          p.productIterator.zip(levels.asInstanceOf[Product].productIterator).map(t => t._1 match {
            case i: Int => getNoiseGenerator(t._2.asInstanceOf[Int], i.toDouble / split)
            case d: Double => getNoiseGenerator(t._2.asInstanceOf[Int], d.toDouble / split)
            case l: Long => getNoiseGenerator(t._2.asInstanceOf[Int], l.toDouble / split)
            case s: Short => getNoiseGenerator(t._2.asInstanceOf[Int], s.toDouble / split)
            case _ => 0
          }).toVector
        case i: Iterable[_] => i.zip(levels.asInstanceOf[Iterable[_]]).map(t => t._1 match {
          case i: Int => getNoiseGenerator(t._2.asInstanceOf[Int], i.toDouble / split)
          case d: Double => getNoiseGenerator(t._2.asInstanceOf[Int], d.toDouble / split)
          case l: Long => getNoiseGenerator(t._2.asInstanceOf[Int], l.toDouble / split)
          case s: Short => getNoiseGenerator(t._2.asInstanceOf[Int], s.toDouble / split)
          case _ => 0
        }).toVector
        case i: DenseVector[Double] => i.toArray.zip(levels.asInstanceOf[Iterable[_]]).map(t => t._1 match {
          case d: Double => getNoiseGenerator(t._2.asInstanceOf[Int], d.toDouble / split)
          case _ => 0
        }).toVector
        case i: Int => getNoiseGenerator(levels.asInstanceOf[Int], i.toDouble / split)
        case d: Double => getNoiseGenerator(levels.asInstanceOf[Int], d.toDouble / split)
        case l: Long => getNoiseGenerator(levels.asInstanceOf[Int], l.toDouble / split)
        case s: Short => getNoiseGenerator(levels.asInstanceOf[Int], s.toDouble / split)
        case _ => 0
      }
      val key = t._1 match {
        case p: Product => p.productElement(0)
        case i: Iterable[_] => i.head
        case i: DenseVector[Double] => i(0)
        case _ => t._1
      }

      (key, DPAggregator.com_noise_tuple(t._1, z))
    }
    ).toVector.sortBy(_._1)(AnyOrder).map(_._2)
  }
}

// without any noise
class NoneDPModel extends DPModel {
  override def final_op[T](iterable: Iterable[(Any, Any)], in: scala.Vector[Array[(T, Any)]]): Iterable[Any] = {
    iterable.toVector.map(_._1).sortBy(t => {
      t match {
        case p: Product => p.productElement(0)
        case i: Iterable[_] => i.head
        case i: DenseVector[Double] => i(0)
        case _ => t
      }
    })(AnyOrder)
  }
}
