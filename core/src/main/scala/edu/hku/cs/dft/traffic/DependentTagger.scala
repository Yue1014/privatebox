package edu.hku.cs.dft.traffic

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import edu.hku.cs.dft.datamodel.DataOperation
import edu.hku.cs.dft.optimization.RuleMaker

/**
  * Created by max on 22/3/2017.
  */

/**
  * A [[DependentTagger]] use dependent key as the hash key, and add
  * this key to all the mapper
  * it use data count as r value in the implementation
*/

class DependentTagger extends PartitionSchemeTagger{

  var partitionTags: Map[String, Set[PartitionScheme]] = Map()

  var choosenSchemes: Map[String, PartitionScheme] = Map()

  @deprecated
  def tagScheme_old(): Unit = {
    val emptyScheme = PartitionScheme(0, Set(), 0)
    val checkList = this.shuffleSet
    checkList.foreach(clist => {

      // tag from reducer to the root
      var currentDatas = List(clist)
      var ldata = dataSet(clist)

      var thisTags: Map[String, Set[PartitionScheme]] = Map()

      // we use dataCount as r
      val currentScheme = PartitionScheme(RuleMaker.typeInfoLength(ldata.dataType),
        (1 to ldata.reduceKeyRange).toSet, ldata.dataCount)
      thisTags += clist -> Set(currentScheme)

      var visitedSet: Set[String] = Set()

      var reduceCount = 0

      // tag all partition scheme to the top
      while(currentDatas.nonEmpty) {
        val currentValue = currentDatas.last
        val currentData = this.dataSet(currentValue)
//        visitedSet += currentValue
        if (currentData.op() != DataOperation.None && (reduceCount == 0 || currentData.op() == DataOperation.Map)) {
          val reduceSet = thisTags.getOrElse(currentValue, Set())
          // if it is null, then it came across a problem
          assert(reduceSet.nonEmpty)
          reduceSet.foreach(c => {
            currentData.deps.foreach(dep => {
              // find the dependent keyset
              var addSet = thisTags.getOrElse(dep._1, Set())
              dep._2.foreach(rule => {
                val mapDep = rule._1.toMap
                val datar = dataSet(dep._1)
                // add according to the current rule
                  var depKeys: Set[Int] = Set()
                  c.hashKeySet.foreach(k => {
                    if (mapDep.contains(k)) {
                      depKeys ++= mapDep(k).toSet
                    }
                  })
                  val r = addSet.find(_ == depKeys).getOrElse(emptyScheme).r + datar.dataCount
                  val currentScheme = PartitionScheme(RuleMaker.typeInfoLength(dataSet(dep._1).dataType),
                    depKeys, r)
                  addSet += currentScheme
                  thisTags += dep._1 -> addSet
              })
              if (!visitedSet.contains(dep._1)) {
                currentDatas = dep._1 :: currentDatas
                visitedSet += dep._1
              }
            })
          })
        }
        currentDatas = currentDatas.init
        reduceCount += 1
      }

      // combine the scheme to teh tag
      thisTags.foreach(tags => {
        var gotSchemes = partitionTags.getOrElse(tags._1, Set())
        val foundTag = gotSchemes.find(_ == currentScheme)
        tags._2.foreach(tag => {
          val addScheme: PartitionScheme = if (foundTag.nonEmpty) {
            PartitionScheme(tag.keyCount, tag.hashKeySet, tag.r + foundTag.get.r)
          } else {
            tag
          }
          gotSchemes += addScheme
        })
        partitionTags += tags._1 -> gotSchemes
      })

    })
  }

  override def tagScheme(): Unit = {
    val emptyScheme = PartitionScheme(0, Set(), 0)
    val checkList = this.shuffleSet
    checkList.foreach(clist => {
      var currentDatas = List(clist)
      var ldata = dataSet(clist)

      var thisTags: Map[String, Set[PartitionScheme]] = Map()

      val currentScheme = PartitionScheme(RuleMaker.typeInfoLength(ldata.dataType),
        (1 to ldata.reduceKeyRange).toSet, ldata.dataCount)
      thisTags += clist -> Set(currentScheme)

      var visitedSet: Set[String] = Set()

      var reduceCount = 0
      while (currentDatas.nonEmpty) {
        val currentValue = currentDatas.last
        val currentData = this.dataSet(currentValue)
        val reduceSet = thisTags.getOrElse(currentValue, Set())
        assert(reduceSet.nonEmpty)
        reduceSet.foreach(c => {
          currentData.deps.foreach(dep => {
            var addSet = thisTags.getOrElse(dep._1, Set())
            dep._2.foreach(rule => {
              // do not consider the empty rule
              if (rule._1.nonEmpty) {
                val mapDep = rule._1.toMap
                val datar = dataSet(dep._1)
                // add according to the current rule
                var depKeys: Set[Int] = Set()
                c.hashKeySet.foreach(k => {
                  if (mapDep.contains(k)) {
                    depKeys ++= mapDep(k).toSet
                  }
                })
                /**
                  * Do not consider the empty dependency now, as we think it may be
                  * wire. Currently, it may confuses with input empty dependency,
                  * a proper way to handle this is to add tag in input
                */
                val r = addSet.find(_ == depKeys).getOrElse(emptyScheme).r + datar.dataCount
                val currentScheme = PartitionScheme(RuleMaker.typeInfoLength(dataSet(dep._1).dataType),
                  depKeys, r)
                addSet += currentScheme
                thisTags += dep._1 -> addSet
              }
            })
            if (!visitedSet.contains(dep._1) && dep._2.nonEmpty) {
              currentDatas = dep._1 :: currentDatas
              visitedSet += dep._1
            }
          })
        })
        currentDatas = currentDatas.init
      }

      // combine the scheme to teh tag
      thisTags.foreach(tags => {
        var gotSchemes = partitionTags.getOrElse(tags._1, Set())
        val foundTag = gotSchemes.find(_ == currentScheme)
        tags._2.foreach(tag => {
          val addScheme: PartitionScheme = if (foundTag.nonEmpty) {
            PartitionScheme(tag.keyCount, tag.hashKeySet, tag.r + foundTag.get.r)
          } else {
            tag
          }
          gotSchemes += addScheme
        })
        partitionTags += tags._1 -> gotSchemes
      })

    })
  }

  /**
    * Here [[chooseScheme]] simply choose the smallest dependent set
  */
  override def chooseScheme(): Unit = {
    this.partitionTags.foreach(sp => {
      var scheme = sp._2.head
      sp._2.foreach(s => {
        if (s.hashKeySet.size < scheme.hashKeySet.size) {
          scheme = s
        }
      })
      choosenSchemes += sp._1 -> scheme
    })
  }

  def printScheme(): Unit = {
    this.choosenSchemes.foreach(t => {
      println(t._1 + " " + t._2)
    })
  }

}

object DependentTagger {

  def serializeTagger(path: String, dependentTagger: DependentTagger): Unit = {
    val oos = new ObjectOutputStream(new FileOutputStream(path))
    oos.writeObject(dependentTagger)
  }

  def deserializeTagger(path: String): DependentTagger = {
    val ois = new ObjectInputStream(new FileInputStream(path))
    ois.readObject().asInstanceOf[DependentTagger]
  }

}

object TaggerMain {
  def main(args: Array[String]): Unit = {
    val dependentTagger = DependentTagger.deserializeTagger("analyzer.obj")
    dependentTagger.firstRoundEntry()
    dependentTagger.tagScheme()
    dependentTagger.chooseScheme()
    dependentTagger.printScheme()
    dependentTagger.serializeToFiles("default.scheme")
  }
}