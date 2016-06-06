package org.apache.flink.quickstart

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import org.apache.flink.api.scala._
import scala.util.control.Exception.allCatch

/**
  * Skeleton for a Flink Job.
  *
  * For a full example of a Flink Job, see the WordCountJob.scala file in the
  * same package/directory or have a look at the website.
  *
  * You can also generate a .jar file that you can submit on your Flink
  * cluster. Just type
  * {{{
  *   mvn clean package
  * }}}
  * in the projects root directory. You will find the jar in
  * target/flink-quickstart-0.1-SNAPSHOT-Sample.jar
  *
  */
object Job {
  def main(args: Array[String]) {
    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val pictures = env.readCsvFile[Picture](
      filePath = "src/main/resources/pictures.csv",
      ignoreFirstLine = true,
      pojoFields = Array("name", "year", "nominations", "rating", "duration", "genre1", "genre2", "release", "metacritic", "synopsis")
    )
    //
    //    pictures.print()
    //    pictures.count()
    //
    def isInteger(s: String): Boolean = (allCatch opt s.toInt).isDefined
    //
//    val validNominations = pictures.filter(p => isInteger(p.nominations)).map(p => p.nominations)
//    println(validNominations.reduce(_ + _).collect().head / validNominations.count)
    //
    val validNominations = pictures.filter(p => isInteger(p.nominations)).map(p => Tuple1[Double](p.nominations.toDouble))
    println(validNominations.sum(0).collect().head._1 / validNominations.count())
    //
    //    // execute program
    //    env.execute("Flink Scala API Skeleton")
  }
}

case class Picture(
  val name: String,
  val year: Integer,
  // val nominations:Integer,
  // val nominations:Option[Integer],
  val nominations: String,
  val rating: Double,
  val duration: Integer,
  val genre1: String,
  val genre2: String,
  val release: String,
  val metacritic: String,
  val synopsis: String
)
