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

package com.intel.hibench.sparkbench.micro

import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.spark.{SparkConf, SparkContext}

/*
 * Adopted from spark's example: https://spark.apache.org/examples.html
 */
object ScalaWordCount{
  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaWordCount <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaWordCount") // SSY importing system config by default
    val sc = new SparkContext(sparkConf)
		// SSY ../HiBench/sparkbench/common/src/main/scala/com/intel/hibench/sparkbench/common/IOCommon.scala
    val io = new IOCommon(sc)
    // SSY data is a HadoopRDD returned from ../spark/core/src/main/scala/org/apache/spark/SparkContext.scala hadoopRDD extending RDD
		// SSY load function call map of sc, so the result is still a MapPartitionsRDD
    val data = io.load[String](args(0))
    // SSY ../spark/core/src/main/scala/org/apache/spark/rdd/RDD.scala
    // it seems that the RDD is the center of the spark running mechanism that synchronize multiple machine
    val counts = data.flatMap(line => line.split(" ")) // MapPartitionsRDD
                     .map(word => (word, 1)) // MapPartitionsRDD, but according to core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala, this may be converted implictly to PairRDDFunctions
                     .reduceByKey(_ + _)  // SSY core/src/main/scala/org/apache/spark/rdd/PairRDDFunctions.scala
		// SSY ../HiBench/sparkbench/common/src/main/scala/com/intel/hibench/sparkbench/common/IOCommon.scala
		// SSY io.save call data.saveAsTextFile as action according to
		// https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations
		// SSY ../spark/core/src/main/scala/org/apache/spark/rdd/RDD.scala
    io.save(args(1), counts) // SSY refer to count
    sc.stop()
  }
}
