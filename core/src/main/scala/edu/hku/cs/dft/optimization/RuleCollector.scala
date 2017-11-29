package edu.hku.cs.dft.optimization

import edu.hku.cs.dft.optimization.RuleCollector.{Rule, RuleSet}

/**
  * Created by jianyu on 3/4/17.
  */

/**
  * A [[RuleCollector]] collect the rule infer in a rdd operation
  * its id should be the rdd id of spark
*/

class RuleCollector(id: Int) {
  private var ruleSet: RuleSet = Map()

  def addRule(r: Rule): Unit = {
    val found = ruleSet.find(_._1 == r)
    val returnVal = if (found.isEmpty) {
      r -> 1
    } else {
      r -> (found.get._2 + 1)
    }
    ruleSet += returnVal
  }

  def isEmpty(): Boolean = ruleSet.isEmpty

  def collect(): Map[Rule, Int] = ruleSet
}

object RuleCollector {
  type Dependency = (Int, List[Int])
  type Rule = List[Dependency]
  type RuleSet = Map[Rule, Int]

  def equalsList(listA: List[Int], listB: List[Int]): Boolean = {
    if (listA.length != listB.length) {
      false
    } else {
      listA.zip(listB).count(t => t._1 == t._2) == listA.length
    }
  }

  def equalsRule(ruleA: Rule, ruleB: Rule): Boolean = {
    if (ruleA.length != ruleB.length){
      false
    } else {
      val mA = ruleA.toMap
      val mB = ruleB.toMap
      mB.foreach(t => {
        if (!mA.contains(t._1))
          return false
        if (!equalsList(t._2, mA(t._1)))
          return false
      })
      true
    }
  }

  def CombineRule(rule_a: RuleSet, rule_b: RuleSet): RuleSet = {
    var returnVal = rule_a
    rule_b.foreach(rule => {
      val found = rule_a.find(t => equalsRule(t._1, rule._1))
      if (found.isEmpty) {
        returnVal += rule
      } else {
        returnVal += rule._1 -> (rule._2 + found.get._2)
      }
    })
    returnVal
  }
}
