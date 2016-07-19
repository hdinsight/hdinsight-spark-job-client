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

package com.microsoft.spark.client

import com.microsoft.livy.client.argument.parser._
import com.microsoft.livy.client.batch.job.LivyBatchClient
import com.microsoft.livy.client.common._

object SparkClientMain {

  def main(inputArguments: Array[String]) {

    val inputOptions: LivyClientArgumentParser.ArgumentMap
    = LivyClientArgumentParser.parseArguments(Map(), inputArguments.toList)

    //println(inputOptions)

    val clientOptions: LivyClientArgumentParser.ClientOptions
    = LivyClientArgumentParser.verifyUpdateArguments(inputOptions)

    //println(clientOptions)

    clientOptions(LivyClientMode) match {

      case LivyClientMode.Batch => {
        clientOptions(LivyClientAction) match {
          case LivyClientAction.List => LivyBatchClient.list(inputOptions)
          case LivyClientAction.Submit => LivyBatchClient.submit(inputOptions)
          case LivyClientAction.Run => LivyBatchClient.monitor(inputOptions, LivyBatchClient.submit(inputOptions))
          case LivyClientAction.Monitor => LivyBatchClient.monitor(inputOptions, -1)
          case LivyClientAction.Kill => LivyBatchClient.kill(inputOptions)
        }
      }

      case LivyClientMode.Interactive => throw new NotImplementedError()
    }
  }
}