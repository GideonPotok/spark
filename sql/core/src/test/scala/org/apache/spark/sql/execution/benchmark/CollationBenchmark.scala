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
package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.functions._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Benchmark to measure performance for joins. To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.CollationBenchmark"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/JoinBenchmark-results.txt".
 * }}}
 */

object CollationBenchmark extends SqlBasedBenchmark {
  private val collationTypes = Seq("UTF8_BINARY", "UTF8_BINARY_LCASE", "UNICODE", "UNICODE_CI")

  def generateUTF8Strings(n: Int): Seq[UTF8String] = {
    // Generate n UTF8Strings
    (1 to n).map(i => UTF8String.fromString(i.toString))
  }

  def benchmarkFilter(collationTypes: Seq[String], utf8Strings: Seq[UTF8String]): Unit = {
    val benchmark = collationTypes.foldLeft(
      new Benchmark(s"filter collation types", utf8Strings.size, output = output)) {
      (b, collationType) =>
        b.addCase(s"filter - $collationType") { _ =>
          val collation = CollationFactory.fetchCollation(collationType)
          utf8Strings.filter(s =>
            collation.equalsFunction(s, UTF8String.fromString("500")).booleanValue())
        }
        b.addCase(s"hashFunction - $collationType") { _ =>
          val collation = CollationFactory.fetchCollation(collationType)
          utf8Strings.map(s => collation.hashFunction.applyAsLong(s))
        }
        b
    }
    benchmark.run()
  }

  def collationBenchmarkFilterEqual(
      collationTypes: Seq[String],
      utf8Strings: Seq[UTF8String]): Unit = {
    val N = 4 << 20
    val benchmark = collationTypes.foldLeft(
      new Benchmark(s"filter df column with collation", utf8Strings.size, output = output)) {
      (b, collationType) =>
        b.addCase(s"filter df column with collation - $collationType") { _ =>
          val map: Map[String, Column] = utf8Strings.map(_.toString).zipWithIndex.map{
            case (s, i) =>
              (s"s${i.toString}", expr(s"collate('${s.toString()}', '$collationType')"))
          }.toMap

          val df = spark
            .range(N)
            .withColumn("id_s", expr("cast(id as string)"))
            .selectExpr((Seq("id_s") ++ collationTypes.map(t =>
              s"collate(id_s, '$collationType') as k_$t")): _*)
//            .withColumn("k_lower", expr("lower(id_s)"))
//            .withColumn("k_upper", expr("upper(id_s)"))
            .withColumns(map)
          utf8Strings.map(_.toString).zipWithIndex.foreach {
            case (s, i) =>
              df.where(col(s"k_$collationType") === col(s"s${i.toString}"))
                .queryExecution.executedPlan.executeCollect()
            //          .write.mode("overwrite").format("noop").save()

          }
        }
        b
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val utf8Strings = generateUTF8Strings(1000) // Adjust the size as needed
    collationBenchmarkFilterEqual(collationTypes.reverse, utf8Strings.slice(0, 1))
    benchmarkFilter(collationTypes, utf8Strings)
  }
}
