/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.statsEstimation

import java.sql.Date

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.types._

/**
 * In this test suite, we test predicates containing the following operators:
 * =, <, <=, >, >=, AND, OR, IS NULL, IS NOT NULL, IN, NOT IN
 */
class FilterEstimationSuite extends StatsEstimationTestBase {

  // Suppose our test table has 10 rows and 6 columns.
  // First column cint has values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
  // Hence, distinctCount:10, min:1, max:10, nullCount:0, avgLen:4, maxLen:4
  val arInt = AttributeReference("cint", IntegerType)()
  val childColStatInt = ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
    nullCount = 0, avgLen = 4, maxLen = 4)

  // only 2 values
  val arBool = AttributeReference("cbool", BooleanType)()
  val childColStatBool = ColumnStat(distinctCount = 2, min = Some(false), max = Some(true),
    nullCount = 0, avgLen = 1, maxLen = 1)

  // Second column cdate has 10 values from 2017-01-01 through 2017-01-10.
  val dMin = Date.valueOf("2017-01-01")
  val dMax = Date.valueOf("2017-01-10")
  val arDate = AttributeReference("cdate", DateType)()
  val childColStatDate = ColumnStat(distinctCount = 10, min = Some(dMin), max = Some(dMax),
    nullCount = 0, avgLen = 4, maxLen = 4)

  // Fourth column cdecimal has 4 values from 0.20 through 0.80 at increment of 0.20.
  val decMin = new java.math.BigDecimal("0.200000000000000000")
  val decMax = new java.math.BigDecimal("0.800000000000000000")
  val arDecimal = AttributeReference("cdecimal", DecimalType(18, 18))()
  val childColStatDecimal = ColumnStat(distinctCount = 4, min = Some(decMin), max = Some(decMax),
    nullCount = 0, avgLen = 8, maxLen = 8)

  // Fifth column cdouble has 10 double values: 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0
  val arDouble = AttributeReference("cdouble", DoubleType)()
  val childColStatDouble = ColumnStat(distinctCount = 10, min = Some(1.0), max = Some(10.0),
    nullCount = 0, avgLen = 8, maxLen = 8)

  // Sixth column cstring has 10 String values:
  // "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9"
  val arString = AttributeReference("cstring", StringType)()
  val childColStatString = ColumnStat(distinctCount = 10, min = None, max = None,
    nullCount = 0, avgLen = 2, maxLen = 2)

  test("cint = 2") {
    validateEstimatedStats(
      arInt,
      Filter(EqualTo(arInt, Literal(2)), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 1, min = Some(2), max = Some(2),
        nullCount = 0, avgLen = 4, maxLen = 4),
      1)
  }

  test("cint <=> 2") {
    validateEstimatedStats(
      arInt,
      Filter(EqualNullSafe(arInt, Literal(2)), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 1, min = Some(2), max = Some(2),
        nullCount = 0, avgLen = 4, maxLen = 4),
      1)
  }

  test("cint = 0") {
    // This is an out-of-range case since 0 is outside the range [min, max]
    validateEstimatedStats(
      arInt,
      Filter(EqualTo(arInt, Literal(0)), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      0)
  }

  test("cint < 3") {
    validateEstimatedStats(
      arInt,
      Filter(LessThan(arInt, Literal(3)), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 2, min = Some(1), max = Some(3),
        nullCount = 0, avgLen = 4, maxLen = 4),
      3)
  }

  test("cint < 0") {
    // This is a corner case since literal 0 is smaller than min.
    validateEstimatedStats(
      arInt,
      Filter(LessThan(arInt, Literal(0)), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      0)
  }

  test("cint <= 3") {
    validateEstimatedStats(
      arInt,
      Filter(LessThanOrEqual(arInt, Literal(3)), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 2, min = Some(1), max = Some(3),
        nullCount = 0, avgLen = 4, maxLen = 4),
      3)
  }

  test("cint > 6") {
    validateEstimatedStats(
      arInt,
      Filter(GreaterThan(arInt, Literal(6)), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 4, min = Some(6), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      5)
  }

  test("cint > 10") {
    // This is a corner case since max value is 10.
    validateEstimatedStats(
      arInt,
      Filter(GreaterThan(arInt, Literal(10)), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      0)
  }

  test("cint >= 6") {
    validateEstimatedStats(
      arInt,
      Filter(GreaterThanOrEqual(arInt, Literal(6)), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 4, min = Some(6), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      5)
  }

  test("cint IS NULL") {
    validateEstimatedStats(
      arInt,
      Filter(IsNull(arInt), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 0, min = None, max = None,
        nullCount = 0, avgLen = 4, maxLen = 4),
      0)
  }

  test("cint IS NOT NULL") {
    validateEstimatedStats(
      arInt,
      Filter(IsNotNull(arInt), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      10)
  }

  test("cint > 3 AND cint <= 6") {
    val condition = And(GreaterThan(arInt, Literal(3)), LessThanOrEqual(arInt, Literal(6)))
    validateEstimatedStats(
      arInt,
      Filter(condition, childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 3, min = Some(3), max = Some(6),
        nullCount = 0, avgLen = 4, maxLen = 4),
      4)
  }

  test("cint = 3 OR cint = 6") {
    val condition = Or(EqualTo(arInt, Literal(3)), EqualTo(arInt, Literal(6)))
    validateEstimatedStats(
      arInt,
      Filter(condition, childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      2)
  }

  test("cint IN (3, 4, 5)") {
    validateEstimatedStats(
      arInt,
      Filter(InSet(arInt, Set(3, 4, 5)), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 3, min = Some(3), max = Some(5),
        nullCount = 0, avgLen = 4, maxLen = 4),
      3)
  }

  test("cint NOT IN (3, 4, 5)") {
    validateEstimatedStats(
      arInt,
      Filter(Not(InSet(arInt, Set(3, 4, 5))), childStatsTestPlan(Seq(arInt), 10L)),
      ColumnStat(distinctCount = 10, min = Some(1), max = Some(10),
        nullCount = 0, avgLen = 4, maxLen = 4),
      7)
  }

  test("cbool = true") {
    validateEstimatedStats(
      arBool,
      Filter(EqualTo(arBool, Literal(true)), childStatsTestPlan(Seq(arBool), 10L)),
      ColumnStat(distinctCount = 1, min = Some(true), max = Some(true),
        nullCount = 0, avgLen = 1, maxLen = 1),
      5)
  }

  test("cbool > false") {
    // bool comparison is not supported yet, so stats remain same.
    validateEstimatedStats(
      arBool,
      Filter(GreaterThan(arBool, Literal(false)), childStatsTestPlan(Seq(arBool), 10L)),
      ColumnStat(distinctCount = 2, min = Some(false), max = Some(true),
        nullCount = 0, avgLen = 1, maxLen = 1),
      10)
  }

  test("cdate = cast('2017-01-02' AS DATE)") {
    val d20170102 = Date.valueOf("2017-01-02")
    validateEstimatedStats(
      arDate,
      Filter(EqualTo(arDate, Literal(d20170102)),
        childStatsTestPlan(Seq(arDate), 10L)),
      ColumnStat(distinctCount = 1, min = Some(d20170102), max = Some(d20170102),
        nullCount = 0, avgLen = 4, maxLen = 4),
      1)
  }

  test("cdate < cast('2017-01-03' AS DATE)") {
    val d20170103 = Date.valueOf("2017-01-03")
    validateEstimatedStats(
      arDate,
      Filter(LessThan(arDate, Literal(d20170103)),
        childStatsTestPlan(Seq(arDate), 10L)),
      ColumnStat(distinctCount = 2, min = Some(dMin), max = Some(d20170103),
        nullCount = 0, avgLen = 4, maxLen = 4),
      3)
  }

  test("""cdate IN ( cast('2017-01-03' AS DATE),
      cast('2017-01-04' AS DATE), cast('2017-01-05' AS DATE) )""") {
    val d20170103 = Date.valueOf("2017-01-03")
    val d20170104 = Date.valueOf("2017-01-04")
    val d20170105 = Date.valueOf("2017-01-05")
    validateEstimatedStats(
      arDate,
      Filter(In(arDate, Seq(Literal(d20170103), Literal(d20170104), Literal(d20170105))),
        childStatsTestPlan(Seq(arDate), 10L)),
      ColumnStat(distinctCount = 3, min = Some(d20170103), max = Some(d20170105),
        nullCount = 0, avgLen = 4, maxLen = 4),
      3)
  }

  test("cdecimal = 0.400000000000000000") {
    val dec_0_40 = new java.math.BigDecimal("0.400000000000000000")
    validateEstimatedStats(
      arDecimal,
      Filter(EqualTo(arDecimal, Literal(dec_0_40)),
        childStatsTestPlan(Seq(arDecimal), 4L)),
      ColumnStat(distinctCount = 1, min = Some(dec_0_40), max = Some(dec_0_40),
        nullCount = 0, avgLen = 8, maxLen = 8),
      1)
  }

  test("cdecimal < 0.60 ") {
    val dec_0_60 = new java.math.BigDecimal("0.600000000000000000")
    validateEstimatedStats(
      arDecimal,
      Filter(LessThan(arDecimal, Literal(dec_0_60)),
        childStatsTestPlan(Seq(arDecimal), 4L)),
      ColumnStat(distinctCount = 3, min = Some(decMin), max = Some(dec_0_60),
        nullCount = 0, avgLen = 8, maxLen = 8),
      3)
  }

  test("cdouble < 3.0") {
    validateEstimatedStats(
      arDouble,
      Filter(LessThan(arDouble, Literal(3.0)), childStatsTestPlan(Seq(arDouble), 10L)),
      ColumnStat(distinctCount = 2, min = Some(1.0), max = Some(3.0),
        nullCount = 0, avgLen = 8, maxLen = 8),
      3)
  }

  test("cstring = 'A2'") {
    validateEstimatedStats(
      arString,
      Filter(EqualTo(arString, Literal("A2")), childStatsTestPlan(Seq(arString), 10L)),
      ColumnStat(distinctCount = 1, min = None, max = None,
        nullCount = 0, avgLen = 2, maxLen = 2),
      1)
  }

  // There is no min/max statistics for String type.  We estimate 10 rows returned.
  test("cstring < 'A2'") {
    validateEstimatedStats(
      arString,
      Filter(LessThan(arString, Literal("A2")), childStatsTestPlan(Seq(arString), 10L)),
      ColumnStat(distinctCount = 10, min = None, max = None,
        nullCount = 0, avgLen = 2, maxLen = 2),
      10)
  }

  // This is a corner test case.  We want to test if we can handle the case when the number of
  // valid values in IN clause is greater than the number of distinct values for a given column.
  // For example, column has only 2 distinct values 1 and 6.
  // The predicate is: column IN (1, 2, 3, 4, 5).
  test("cint IN (1, 2, 3, 4, 5)") {
    val cornerChildColStatInt = ColumnStat(distinctCount = 2, min = Some(1), max = Some(6),
      nullCount = 0, avgLen = 4, maxLen = 4)
    val cornerChildStatsTestplan = StatsTestPlan(
      outputList = Seq(arInt),
      rowCount = 2L,
      attributeStats = AttributeMap(Seq(arInt -> cornerChildColStatInt))
    )
    validateEstimatedStats(
      arInt,
      Filter(InSet(arInt, Set(1, 2, 3, 4, 5)), cornerChildStatsTestplan),
      ColumnStat(distinctCount = 2, min = Some(1), max = Some(5),
        nullCount = 0, avgLen = 4, maxLen = 4),
      2)
  }

  private def childStatsTestPlan(outList: Seq[Attribute], tableRowCount: BigInt): StatsTestPlan = {
    StatsTestPlan(
      outputList = outList,
      rowCount = tableRowCount,
      attributeStats = AttributeMap(Seq(
        arInt -> childColStatInt,
        arBool -> childColStatBool,
        arDate -> childColStatDate,
        arDecimal -> childColStatDecimal,
        arDouble -> childColStatDouble,
        arString -> childColStatString
      ))
    )
  }

  private def validateEstimatedStats(
      ar: AttributeReference,
      filterNode: Filter,
      expectedColStats: ColumnStat,
      rowCount: Int): Unit = {

    val expectedAttrStats = toAttributeMap(Seq(ar.name -> expectedColStats), filterNode)
    val expectedSizeInBytes = getOutputSize(filterNode.output, rowCount, expectedAttrStats)

    val filteredStats = filterNode.stats(conf)
    assert(filteredStats.sizeInBytes == expectedSizeInBytes)
    assert(filteredStats.rowCount.get == rowCount)
    assert(filteredStats.attributeStats(ar) == expectedColStats)

    // If the filter has a binary operator (including those nested inside
    // AND/OR/NOT), swap the sides of the attribte and the literal, reverse the
    // operator, and then check again.
    val rewrittenFilter = filterNode transformExpressionsDown {
      case EqualTo(ar: AttributeReference, l: Literal) =>
        EqualTo(l, ar)

      case LessThan(ar: AttributeReference, l: Literal) =>
        GreaterThan(l, ar)
      case LessThanOrEqual(ar: AttributeReference, l: Literal) =>
        GreaterThanOrEqual(l, ar)

      case GreaterThan(ar: AttributeReference, l: Literal) =>
        LessThan(l, ar)
      case GreaterThanOrEqual(ar: AttributeReference, l: Literal) =>
        LessThanOrEqual(l, ar)
    }

    if (rewrittenFilter != filterNode) {
      validateEstimatedStats(ar, rewrittenFilter, expectedColStats, rowCount)
    }
  }
}
