package edu.hku.cs.dft.dp

import java.io._

import breeze.linalg.DenseVector

// show error when iterable is in a tuple
object DPAggregator {

  def noise_magnitude(dp: Iterable[Any], origin: Iterable[Any], delta: Any): Any = {
    val sum = dp.zip(origin).map(t => com_two(DPAggregator.com_minus_abs)(t._1, t._2)).reduceLeft((x, y) => com_two(com_add(0))(x, y))
    val n = dp.size
    com_two(com_div_same)(com_div_tuple(sum, n), delta)
  }

  def to_double(a: Any): Any = {
    a match {
      case i: Int => i.toDouble
      case d: Double => d
      case l: Long => l.toDouble
      case s: Short => s.toDouble
      case _ => a
    }
  }

  def noise_accuracy(dp: Iterable[Any], origin: Iterable[Any], delta: Any): Any = {
    val d = com_div_tuple(com_one(com_abs)(delta), 5)
    val sum = dp.zip(origin).map(t => {
      val max_v = com_one(to_double)(com_two(com_add(0))(t._2, d))
      val min_v = com_one(to_double)(com_two(com_minus)(t._2, d))
      com_range(t._1, min_v, max_v)
    }).reduceLeft((x, y) => com_two(com_add(0))(x, y))
    val size = dp.size
    com_div_tuple(sum, size)
  }

  def com_two[T](g: (Any, Any) => Any)( a: T, b: T): Any = {
    a match {
      case (_1, _2, _3, _4, _5) =>
        val b_f = b.asInstanceOf[(_, _, _, _, _)]
        (g(_1, b_f._1), g(_2, b_f._2), g(_3, b_f._3), g(_4, b_f._4), g(_5, b_f._5))
      case (_1, _2, _3, _4) =>
        val b_f = b.asInstanceOf[(_, _, _, _)]
        (g(_1, b_f._1), g(_2, b_f._2), g(_3, b_f._3), g(_4, b_f._4))
      case (_1, _2, _3) =>
        val b_f = b.asInstanceOf[(_, _, _)]
        (g(_1, b_f._1), g(_2, b_f._2), g(_3, b_f._3))
      case (_1, _2) =>
        val b_f = b.asInstanceOf[(_, _)]
        (g(_1, b_f._1), g(_2, b_f._2))
      case d: DenseVector[Double] => d.toArray.zip(b.asInstanceOf[DenseVector[Double]].toArray).map(t => g(t._1, t._2)).toVector
      case i: Iterable[_] =>
        var v: Iterable[_] = b match {
          case i: Iterable[_] => i
          case dv: DenseVector[Double] => dv.toArray
        }
        i.zip(v).map(t => g(t._1, t._2)).toVector
      case _1 => g(_1, b)
    }
  }

  def range_one(a: Any, down: Any, up: Any): Any = {
    a match {
      case i: Int => if (i >= down.asInstanceOf[Double] && i <= up.asInstanceOf[Double]) 1 else 0
      case i: Double => if (i >= down.asInstanceOf[Double] && i <= up.asInstanceOf[Double]) 1 else 0
      case _ => 1
    }
  }

  def com_range( a: Any, down: Any, up: Any): Any = {
    a match {
      case (_1, _2, _3, _4, _5) =>
        val down_f = down.asInstanceOf[(_, _, _, _, _)]
        val up_f = up.asInstanceOf[(_, _, _, _, _)]
        (range_one(_1, down_f._1, up_f._1), range_one(_2, down_f._2, up_f._2), range_one(_3, down_f._3, up_f._3),
          range_one(_4, down_f._4, up_f._4), range_one(_5, down_f._5, up_f._5))
      case (_1, _2, _3, _4) =>
        val down_f = down.asInstanceOf[(_, _, _, _)]
        val up_f = up.asInstanceOf[(_, _, _, _)]
        (range_one(_1, down_f._1, up_f._1), range_one(_2, down_f._2, up_f._2), range_one(_3, down_f._3, up_f._3),
          range_one(_4, down_f._4, up_f._4))
      case (_1, _2, _3) =>
        val down_f = down.asInstanceOf[(_, _, _)]
        val up_f = up.asInstanceOf[(_, _, _)]
        (range_one(_1, down_f._1, up_f._1), range_one(_2, down_f._2, up_f._2), range_one(_3, down_f._3, up_f._3))
      case (_1, _2) =>
        val down_f = down.asInstanceOf[(_, _)]
        val up_f = up.asInstanceOf[(_, _)]
        (range_one(_1, down_f._1, up_f._1), range_one(_2, down_f._2, up_f._2))
      case i: Iterable[_] =>
        val down_f = down.asInstanceOf[Iterable[_]]
        val up_f = up.asInstanceOf[Iterable[_]]
        i.zip(down_f.zip(up_f)).map(t => range_one(t._1, t._2._1, t._2._2)).toVector
      case _1 => range_one(_1, down, up)
    }
  }

  def com_add(n: Int)(a: Any, b: Any): Any = {
    a match {
      case i: Int =>
        b match {
          case dd: Double => dd + i.toDouble
          case ii: Int => i + ii
          case _ => throw new IllegalArgumentException
        }
      case d: Double =>
        val dv = b match {
          case dd: Double => dd
          case ii: Int => ii.toDouble
          case _ => throw new IllegalArgumentException()
        }
        d + dv
      case l: Long => l + b.asInstanceOf[Long]
      case s: Short => s + b.asInstanceOf[Short]
      case g: Any => g
    }
  }

  def com_abs(g: Any): Any = {
    g match {
      case i: Int => math.abs(i)
      case d: Double => math.abs(d)
      case long: Long => math.abs(long)
      case short: Short => math.abs(short)
      case _ => g
    }
  }

  def com_one(g: Any => Any)(a: Any): Any = {
    a match {
      case (_1, _2, _3, _4, _5) =>
        (g(_1), g(_2), g(_3), g(_4), g(_5))
      case (_1, _2, _3, _4) =>
        (g(_1), g(_2), g(_3), g(_4))
      case (_1, _2, _3) =>
        (g(_1), g(_2), g(_3))
      case (_1, _2) =>
        (g(_1), g(_2))
      case i:Iterable[_] =>
        i.map(t => g(t)).toVector
      case _1 => g(_1)
    }
  }

  def com_minus(a: Any, b: Any): Any = {
    a match {
      case i: Int => i
        b match {
          case dd: Double => i.toDouble - dd
          case ii: Int => ii - i
          case _ => throw new IllegalArgumentException
        }
      case d: Double =>
        val dv = b match {
          case ii: Int => ii.toDouble
          case dd: Double => dd
          case _ => throw new IllegalArgumentException()
        }
        d - dv
      case l: Long => l - b.asInstanceOf[Long]
      case s: Short => s - b.asInstanceOf[Short]
      case g: Any => g
    }
  }

  def com_minus_abs(a: Any, b: Any): Any = {
    a match {
      case i: Int =>
        b match {
          case dd: Double => math.abs(i - dd)
          case ii: Int => math.abs(i - ii)
          case _ => throw new IllegalArgumentException()
        }
      case d: Double =>
        val ri = b match {
          case dd: Double => dd
          case i: Int => i.toDouble
          case _ => throw new IllegalArgumentException()
        }
        math.abs(d - ri)
      case l: Long => math.abs(l - b.asInstanceOf[Long])
      case s: Short => math.abs(s - b.asInstanceOf[Short])
      case g: Any => g
    }
  }

  def com_avg(n: Int)(a: Any, b: Any): Any = {
    a match {
      case i: Int => (i + b.asInstanceOf[Int]) / n
      case d: Double => (d + b.asInstanceOf[Double]) / n
      case l: Long => (l + b.asInstanceOf[Long]) / n
      case s: Short => (s + b.asInstanceOf[Short]) / n
      case g: Any => g
    }
  }

  def com_max(a: Any, b: Any): Any = {
    a match {
      case i: Int => math.max(i, b.asInstanceOf[Int])
      case d: Double => math.max(d, b.asInstanceOf[Double])
      case l: Long => math.max(l, b.asInstanceOf[Long])
      case s: Short => math.max(s, b.asInstanceOf[Short])
      case g: Any => g
    }
  }

  def com_min(a: Any, b: Any): Any = {
    a match {
      case i: Int => math.min(i, b.asInstanceOf[Int])
      case d: Double => math.min(d, b.asInstanceOf[Double])
      case l: Long => math.min(l, b.asInstanceOf[Long])
      case s: Short => math.min(s, b.asInstanceOf[Short])
      case g: Any => g
    }
  }

  def com_div_tuple(a: Any, n: Int): Any = {
    a match {
      case (_1, _2, _3, _4, _5) =>
        (com_div(_1, n), com_div(_2, n), com_div(_3, n), com_div(_4, n), com_div(_5, n))
      case (_1, _2, _3, _4) =>
        (com_div(_1, n), com_div(_2, n), com_div(_3, n), com_div(_4, n))
      case (_1, _2, _3) =>
        (com_div(_1, n), com_div(_2, n), com_div(_3, n))
      case (_1, _2) =>
        (com_div(_1, n), com_div(_2, n))
      case i: Iterable[_] => i.map(t => com_div(t, n))
      case d: DenseVector[Double] => d.toArray.map(t => com_div(t, n))
      case _1 => com_div(_1, n)
    }
  }

  def com_div(a: Any, n: Int): Any = {
    a match {
      case i: Int => i.toDouble / n
      case d: Double => d / n
      case l: Long => l.toDouble / n
      case s: Short => s.toDouble / n
      case g: Any => g
    }
  }

  def com_div_same(a: Any, b: Any): Any = {
    a match {
      case i: Int => i.toDouble / b.asInstanceOf[Int]
      case d: Double =>
        if (d != 0) {
          b match {
            case dd: Double => d / dd
            case i: Int => d / i
            case l: Long => d / l
            case _ => throw new IllegalArgumentException("wrong")
          }
        } else
          0
      case l: Long => l.toDouble / b.asInstanceOf[Long]
      case s: Short => s.toDouble / b.asInstanceOf[Short]
      case g: Any => g
    }
  }

  def com_noise(a: Any, lap: Any): Any = {
    a match {
      case i: Int => i.toDouble + lap.asInstanceOf[LapNoiseGenerator].generate()
      case d: Double => d + lap.asInstanceOf[LapNoiseGenerator].generate()
      case l: Long => l + lap.asInstanceOf[LapNoiseGenerator].generate()
      case s: Short => s + lap.asInstanceOf[LapNoiseGenerator].generate()
      case _ => a
    }
  }

  def com_noise_tuple(a: Any, gen_arr: Any): Any = {
    a match {
      case (_1, _2, _3, _4, _5) =>
        val larr = gen_arr.asInstanceOf[scala.Vector[Any]]
        (_1, com_noise(_2, larr(1)),
          com_noise(_3, larr(2)), com_noise(_4, larr(3)), com_noise(_5, larr(4)))
      case (_1, _2, _3, _4) =>
        val larr = gen_arr.asInstanceOf[scala.Vector[Any]]
        (com_noise(_1, larr(0)), com_noise(_2, larr(1)), com_noise(_3, larr(2)), com_noise(_4, larr(3)))
      case (_1, _2, _3) =>
        val larr = gen_arr.asInstanceOf[scala.Vector[Any]]
        (com_noise(_1, larr(0)), com_noise(_2, larr(1)), com_noise(_3, larr(2)))
      case (_1, _2) =>
        val larr = gen_arr.asInstanceOf[scala.Vector[Any]]
        (com_noise(_1, larr(0)), com_noise(_2, larr(1)))
      case i: Iterable[_] => i.zip(gen_arr.asInstanceOf[Iterable[_]]).map(t => com_noise(t._1, t._2))
      case d: DenseVector[Double] => d.toArray.zip(gen_arr.asInstanceOf[Iterable[_]]).map(t => com_noise(t._1, t._2)).toVector
      case _1 =>
        com_noise(_1, gen_arr)
    }
  }


  def delta[T](iterable: scala.Vector[Array[(T, Any)]]): T = {
    val min_max = iterable.map(t => t.map(g => (g._1, g._1))
      .reduceLeft((a, b) =>
      (DPAggregator.com_two(DPAggregator.com_max)(a._1, b._1).asInstanceOf[T],
       DPAggregator.com_two(DPAggregator.com_min)(a._2, b._2).asInstanceOf[T]))
    ).reduceLeft((x, y) =>
      (DPAggregator.com_two(DPAggregator.com_max)(x._1, y._1).asInstanceOf[T],
       DPAggregator.com_two(DPAggregator.com_min)(x._2, y._2).asInstanceOf[T])
    )
    val delta = DPAggregator.com_two(DPAggregator.com_minus)(min_max._1, min_max._2)
    DPAggregator.com_one(DPAggregator.com_abs)(delta).asInstanceOf[T]
  }

  def dp_aggregate[T](in: scala.Vector[Array[(T, Any)]], dpModel:DPModel): Iterable[Any] = {
    //  calculate the max, min, average
    val n = in.length

    // may add noise here
    val avg_pre = in.map(t => dpModel.pre_average_op(t))

    // calculate the final average result
    var key_values: Map[Any, scala.Vector[(Any, Any)]] = Map()
    avg_pre.foreach(k => {
      val sorted_k = k.toVector.sortBy(t => t._1 match {
        case p: Product => p.productElement(0)
        case i: Iterable[_] => i.head
        case d: DenseVector[Double] => d(0)
        case _ => t._1
      })(AnyOrder)
      var c = 0
      sorted_k.foreach(g => g._1 match {
        case p: Product =>
          var new_orelse = key_values.getOrElse(p.productElement(0), scala.Vector())
          new_orelse ++= scala.Vector(g)
          key_values += p.productElement(0) -> new_orelse
        case i: Iterable[_] =>
          var new_orelse = key_values.getOrElse(i, scala.Vector())
          new_orelse ++= scala.Vector(g)
          key_values += c -> new_orelse
          c += 1
        case d: DenseVector[Double] =>
          var new_orelse = key_values.getOrElse(d, scala.Vector())
          new_orelse ++= scala.Vector(g)
          key_values += d -> new_orelse
        case _ =>
          var new_orelse = key_values.getOrElse(g, scala.Vector())
          new_orelse ++= scala.Vector(g)
          key_values += g -> new_orelse
      })
    })
    val avgAgg =
    key_values.map(g => {
      val nn = g._2.size
      val gg = g._2.reduce((x, y) => (com_two(com_add(nn))(x._1, y._1), com_two(com_max)(x._2, y._2)))
      (com_div_tuple(gg._1, nn), gg._2)
    }
    )
    dpModel.final_op(avgAgg, in)
  }

  def dp_pre_save[T](in: scala.Vector[Array[(T, Any)]], filename: String): Unit = {
    new ObjectOutputStream(new FileOutputStream(new File(filename))).writeObject(in)
  }

  def dp_read[T](filename: String): Unit = {
    val vec = new ObjectInputStream(new FileInputStream(new File(filename))).readObject()
    val a = 0
    a + 1
    println(a)
  }

}

object DPOffline {
  def main(args: Array[String]): Unit = {
    DPAggregator.dp_read(args(0))
  }
}