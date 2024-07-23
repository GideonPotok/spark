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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, UnresolvedWithinGroup}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, Expression, ExpressionDescription, ImplicitCastInputTypes, SortOrder}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.catalyst.util.{ArrayData, CollationFactory, GenericArrayData, UnsafeRowUtils}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, ArrayType, BooleanType, DataType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.OpenHashMap

case class Mode(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0,
    reverseOpt: Option[Boolean] = None)
  extends TypedAggregateWithHashMapAsBuffer with ImplicitCastInputTypes
    with SupportsOrderingWithinGroup with UnaryLike[Expression] {

  def this(child: Expression) = this(child, 0, 0)

  def this(child: Expression, reverse: Boolean) = {
    this(child, 0, 0, Some(reverse))
  }

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def prettyName: String = "mode"

  override def update(
      buffer: OpenHashMap[AnyRef, Long],
      input: InternalRow): OpenHashMap[AnyRef, Long] = {
    val key = child.eval(input)

    if (key != null) {
      buffer.changeValue(InternalRow.copyValue(key).asInstanceOf[AnyRef], 1L, _ + 1L)
    }
    buffer
  }

  override def merge(
      buffer: OpenHashMap[AnyRef, Long],
      other: OpenHashMap[AnyRef, Long]): OpenHashMap[AnyRef, Long] = {
    other.foreach { case (key, count) =>
      buffer.changeValue(key, count, _ + count)
    }
    buffer
  }

  private def recursivelyGetBufferForArrayType(
      a: ArrayType,
      data: ArrayData): Seq[Any] = {
    val fToUse: Int => Any = if (a.elementType.isInstanceOf[StructType]) {
      i: Int =>
        recursivelyGetBufferForStructType(
          data.get(i, a.elementType).asInstanceOf[InternalRow].toSeq(
            a.elementType.asInstanceOf[StructType]).zip(
            a.elementType.asInstanceOf[StructType].fields))
    } else if (a.elementType.isInstanceOf[ArrayType]) {
      i: Int =>
        recursivelyGetBufferForArrayType(
          a.elementType.asInstanceOf[ArrayType],
          data.get(i, a.elementType).asInstanceOf[ArrayData])
    } else if (a.elementType.isInstanceOf[StringType] &&
      !UnsafeRowUtils.isBinaryStable(a.elementType)) {
      i: Int =>
        CollationFactory.getCollationKey(
          data.get(i, a.elementType).asInstanceOf[UTF8String],
          a.elementType.asInstanceOf[StringType].collationId)
    } else {
      i: Int => data.get(i, a.elementType)
    }
    (0 until data.numElements()).map(fToUse)
  }

  private def isSpecificStringTypeMatch(field: StructField, fieldName: String): Boolean =
    field.dataType.isInstanceOf[StringType] &&
      !UnsafeRowUtils.isBinaryStable(field.dataType) &&
      field.name == fieldName

  private def isSpecificStructTypeMatch(field: StructField, fieldName: String): Boolean =
    field.dataType.isInstanceOf[StructType] &&
      field.name == fieldName

  private def handleStringType(key: Any, field: StructField): Any = {
    val strKey = key match {
      case k: String => UTF8String.fromString(k)
      case k: UTF8String => k
    }
    CollationFactory.getCollationKey(strKey, field.dataType.asInstanceOf[StringType].collationId)
  }

  private def recursivelyGetBufferForStructType(
      tuples: Seq[(Any, StructField)]): Seq[Any] = {

    val stringTypeFields = tuples.filter(f => isSpecificStringTypeMatch(f._2, f._2.name))
    val structTypeFields = tuples.filter(f => isSpecificStructTypeMatch(f._2, f._2.name))

    tuples.map {
      case (k, field) if stringTypeFields.exists(_._2 == field) =>
        handleStringType(k, field)
      case (k, field) if structTypeFields.exists(_._2 == field) =>
        recursivelyGetBufferForStructType(
          k.asInstanceOf[InternalRow].toSeq(field.dataType.asInstanceOf[StructType]).zip(
            field.dataType.asInstanceOf[StructType].fields))
      case (k, structField: StructField) if structField.dataType.isInstanceOf[ArrayType] &&
        !UnsafeRowUtils.isBinaryStable(structField.dataType.asInstanceOf[ArrayType]) =>
        recursivelyGetBufferForArrayType(
          structField.dataType.asInstanceOf[ArrayType],
          k.asInstanceOf[ArrayData])
      case (k, _) => k
    }
  }

  override def eval(buffer: OpenHashMap[AnyRef, Long]): Any = {
    if (buffer.isEmpty) {
      return null
    }
    /*
      * The Mode class uses special collation awareness logic
      *  to handle string data types with various collations.
      *
      * For string types that don't support binary equality,
      * we create a new map where the keys are the collation keys of the original strings.
      *
      * Keys from the original map are aggregated based on the corresponding collation keys.
      *  The groupMapReduce method groups the entries by collation key and maps each group
      *  to a single value (the sum of the counts), and finally reduces the groups to a single map.
      *
      * The new map is then used in the rest of the Mode evaluation logic.
      */
    val collationAwareBuffer = getCollationAwareBuffer(child.dataType, buffer)

    reverseOpt.map { reverse =>
      val defaultKeyOrdering = if (reverse) {
        PhysicalDataType.ordering(child.dataType).asInstanceOf[Ordering[AnyRef]].reverse
      } else {
        PhysicalDataType.ordering(child.dataType).asInstanceOf[Ordering[AnyRef]]
      }
      val ordering = Ordering.Tuple2(Ordering.Long, defaultKeyOrdering)
      collationAwareBuffer.maxBy { case (key, count) => (count, key) }(ordering)
    }.getOrElse(collationAwareBuffer.maxBy(_._2))._1
  }

  private def getCollationAwareBuffer(
      childDataType: DataType,
      buffer: OpenHashMap[AnyRef, Long]): Iterable[(AnyRef, Long)] = {
    childDataType match {
      // Short-circuit if there is no collation.
      case _ if UnsafeRowUtils.isBinaryStable(child.dataType) => buffer
      case c: StringType => getCollationAwareBufferForStringType(c, buffer)
      case at: ArrayType => getBufferForArrayType(buffer, at)
      case st: StructType => getBufferForStructType(buffer, st)
      // Not supported: MapType
      case _ => buffer
    }
  }

  private def getBufferForStructType(
      buffer: OpenHashMap[AnyRef, Long],
      st: StructType): Iterable[(AnyRef, Long)] = {
    buffer.groupMapReduce {
      case (key: InternalRow, _) =>
        recursivelyGetBufferForStructType(key.toSeq(st)
          .zip(st.fields))
    }(x => x)((x, y) => (x._1, x._2 + y._2)).values
  }

  private def getBufferForArrayType(
      buffer: OpenHashMap[AnyRef, Long],
      at: ArrayType): Iterable[(AnyRef, Long)] = {
    buffer.groupMapReduce {
      case (key: ArrayData, _) =>
        recursivelyGetBufferForArrayType(at, key)
    }(x => x)((x, y) => (x._1, x._2 + y._2)).values
  }

  private def getCollationAwareBufferForStringType(
      c: StringType,
      buffer: OpenHashMap[AnyRef, Long]) = {
    buffer.toSeq.groupMapReduce {
      case (k, _) => CollationFactory.getCollationKey(k.asInstanceOf[UTF8String], c.collationId)
    }(x => x)((x, y) => (x._1, x._2 + y._2)).values
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): Mode =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): Mode =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def sql(isDistinct: Boolean): String = {
    reverseOpt.map {
      reverse =>
        if (reverse) {
          s"$prettyName() WITHIN GROUP (ORDER BY ${child.sql} DESC)"
        } else {
          s"$prettyName() WITHIN GROUP (ORDER BY ${child.sql})"
        }
    }.getOrElse(super.sql(isDistinct))
  }

  override def orderingFilled: Boolean = child != UnresolvedWithinGroup

  assert(orderingFilled || (!orderingFilled && reverseOpt.isEmpty))

  override def withOrderingWithinGroup(orderingWithinGroup: Seq[SortOrder]): AggregateFunction = {
    child match {
      case UnresolvedWithinGroup =>
        if (orderingWithinGroup.length != 1) {
          throw QueryCompilationErrors.wrongNumOrderingsForInverseDistributionFunctionError(
            nodeName, 1, orderingWithinGroup.length)
        }
        orderingWithinGroup.head match {
          case SortOrder(child, Ascending, _, _) =>
            this.copy(child = child, reverseOpt = Some(true))
          case SortOrder(child, Descending, _, _) =>
            this.copy(child = child, reverseOpt = Some(false))
        }
      case _ => this
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

// TODO: SPARK-48701: PandasMode (all collations)
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(col[, deterministic]) - Returns the most frequent value for the values within `col`. NULL values are ignored. If all the values are NULL, or there are 0 rows, returns NULL.
      When multiple values have the same greatest frequency then either any of values is returned if `deterministic` is false or is not defined, or the lowest value is returned if `deterministic` is true.
    _FUNC_() WITHIN GROUP (ORDER BY col) - Returns the most frequent value for the values within `col` (specified in ORDER BY clause). NULL values are ignored.
      If all the values are NULL, or there are 0 rows, returns NULL. When multiple values have the same greatest frequency only one value will be returned.
      The value will be chosen based on sort direction. Return the smallest value if sort direction is asc or the largest value if sort direction is desc from multiple values with the same frequency.""",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (0), (10), (10) AS tab(col);
       10
      > SELECT _FUNC_(col) FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH), (INTERVAL '10' MONTH) AS tab(col);
       0-10
      > SELECT _FUNC_(col) FROM VALUES (0), (10), (10), (null), (null), (null) AS tab(col);
       10
      > SELECT _FUNC_(col, false) FROM VALUES (-10), (0), (10) AS tab(col);
       0
      > SELECT _FUNC_(col, true) FROM VALUES (-10), (0), (10) AS tab(col);
       -10
      > SELECT _FUNC_() WITHIN GROUP (ORDER BY col) FROM VALUES (0), (10), (10) AS tab(col);
       10
      > SELECT _FUNC_() WITHIN GROUP (ORDER BY col) FROM VALUES (0), (10), (10), (20), (20) AS tab(col);
       10
      > SELECT _FUNC_() WITHIN GROUP (ORDER BY col DESC) FROM VALUES (0), (10), (10), (20), (20) AS tab(col);
       20
  """,
  group = "agg_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
object ModeBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    val numArgs = expressions.length
    if (numArgs == 0) {
      Mode(UnresolvedWithinGroup)
    } else if (numArgs == 1) {
      // For compatibility with function calls without WITHIN GROUP.
      Mode(expressions(0))
    } else if (numArgs == 2) {
      // For compatibility with function calls without WITHIN GROUP.
      if (!expressions(1).foldable) {
        throw QueryCompilationErrors.nonFoldableArgumentError(
          funcName, "deterministic", BooleanType)
      }
      val deterministicResult = expressions(1).eval()
      if (deterministicResult == null) {
        throw QueryCompilationErrors.unexpectedNullError("deterministic", expressions(1))
      }
      if (expressions(1).dataType != BooleanType) {
        throw QueryCompilationErrors.unexpectedInputDataTypeError(
          funcName, 2, BooleanType, expressions(1))
      }
      if (deterministicResult.asInstanceOf[Boolean]) {
        new Mode(expressions(0), true)
      } else {
        Mode(expressions(0))
      }
    } else {
      throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(0), numArgs)
    }
  }
}

/**
 * Mode in Pandas' fashion. This expression is dedicated only for Pandas API on Spark.
 * It has two main difference from `Mode`:
 * 1, it accepts NULLs when `ignoreNA` is False;
 * 2, it returns all the modes for a multimodal dataset;
 */
case class PandasMode(
    child: Expression,
    ignoreNA: Boolean = true,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends TypedAggregateWithHashMapAsBuffer
  with ImplicitCastInputTypes with UnaryLike[Expression] {

  def this(child: Expression) = this(child, true, 0, 0)

  def this(child: Expression, ignoreNA: Expression) =
    this(child, PandasAggregate.expressionToIgnoreNA(ignoreNA, "pandas_mode"))

  // Returns empty array for empty inputs
  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(child.dataType, containsNull = !ignoreNA)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def prettyName: String = "pandas_mode"

  override def update(
      buffer: OpenHashMap[AnyRef, Long],
      input: InternalRow): OpenHashMap[AnyRef, Long] = {
    val key = child.eval(input)

    if (key != null) {
      buffer.changeValue(InternalRow.copyValue(key).asInstanceOf[AnyRef], 1L, _ + 1L)
    } else if (!ignoreNA) {
      buffer.changeValue(null, 1L, _ + 1L)
    }
    buffer
  }

  override def merge(
      buffer: OpenHashMap[AnyRef, Long],
      other: OpenHashMap[AnyRef, Long]): OpenHashMap[AnyRef, Long] = {
    other.foreach { case (key, count) =>
      buffer.changeValue(key, count, _ + count)
    }
    buffer
  }

  override def eval(buffer: OpenHashMap[AnyRef, Long]): Any = {
    if (buffer.isEmpty) {
      return new GenericArrayData(Array.empty)
    }

    val modes = collection.mutable.ArrayBuffer.empty[AnyRef]
    var maxCount = -1L
    val iter = buffer.iterator
    while (iter.hasNext) {
      val (key, count) = iter.next()
      if (maxCount < count) {
        modes.clear()
        modes.append(key)
        maxCount = count
      } else if (maxCount == count) {
        modes.append(key)
      }
    }
    new GenericArrayData(modes)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): PandasMode =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): PandasMode =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
