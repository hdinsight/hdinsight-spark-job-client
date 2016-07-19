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

package com.microsoft.livy.client.batch.job

import com.microsoft.livy.client.argument.parser._
import com.microsoft.livy.client.common._

object LivyBatchClient {

  def list(inputOptions: LivyClientArgumentParser.ArgumentMap): Unit = {

    val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))

    val listURL = "https://%s/livy/batches".format(inputOptions(Symbol(LivyClientArgumentKeys.ClusterFQDN)))
    val livySessionList: LivySessionList = LivyBatchJobAction.list(listURL, userCredentials(inputOptions), testMode)
    if (livySessionList != null) LivyClientUtilities.printSessionList(livySessionList)
  }

  def submit(inputOptions: LivyClientArgumentParser.ArgumentMap): Long = {

    var jobId: Long = -1

    if (inputOptions.contains(Symbol(LivyClientArgumentKeys.SubmitJob))
      || inputOptions.contains(Symbol(LivyClientArgumentKeys.RunJob))) {

      val jobRequest: LivyBatchJob = new LivyBatchJob(
        if (inputOptions.contains(Symbol(LivyClientArgumentKeys.ApplicationName)))
          inputOptions(Symbol(LivyClientArgumentKeys.ApplicationName)).toString
        else "Livy",
        inputOptions(Symbol(LivyClientArgumentKeys.ApplicationJAR)).toString,
        inputOptions(Symbol(LivyClientArgumentKeys.ApplicationClass)).toString,
        if (inputOptions.contains(Symbol(LivyClientArgumentKeys.ApplicationArguments)))
          inputOptions(Symbol(LivyClientArgumentKeys.ApplicationArguments)).asInstanceOf[List[String]]
        else List[String](),
        inputOptions(Symbol(LivyClientArgumentKeys.ExecutorCount)).asInstanceOf[Int],
        inputOptions(Symbol(LivyClientArgumentKeys.PerExecutorCoreCount)).asInstanceOf[Int],
        inputOptions(Symbol(LivyClientArgumentKeys.PerExecutorMemoryInGB)).toString + "G",
        inputOptions(Symbol(LivyClientArgumentKeys.DriverMemoryInGB)).toString + "G",
        if (inputOptions.contains(Symbol(LivyClientArgumentKeys.ClasspathJARS)))
          inputOptions(Symbol(LivyClientArgumentKeys.ClasspathJARS)).asInstanceOf[List[String]]
        else List[String](),
        if (inputOptions.contains(Symbol(LivyClientArgumentKeys.ExecutorFiles))) {
          inputOptions(Symbol(LivyClientArgumentKeys.ExecutorFiles)).asInstanceOf[List[String]]
        } else {
          List[String]()
        })

      val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))

      val postURL = "https://%s/livy/batches".format(inputOptions(Symbol(LivyClientArgumentKeys.ClusterFQDN)))

      val livyJobDetails: LivyJobDetails = LivyBatchJobAction.submit(postURL, userCredentials(inputOptions),
        jobRequest, testMode)

      if (livyJobDetails != null) LivyClientUtilities.printJobDetails(livyJobDetails)

      jobId = livyJobDetails.id
    }

    jobId
  }

  def monitor(inputOptions: LivyClientArgumentParser.ArgumentMap, livyJobId: Long): Unit = {

    if (inputOptions.contains(Symbol(LivyClientArgumentKeys.RunJob))
      || inputOptions.contains(Symbol(LivyClientArgumentKeys.MonitorJob))) {

      val jobId = if (livyJobId < 0) inputOptions(Symbol(LivyClientArgumentKeys.JobId)).asInstanceOf[Int]
      else livyJobId

      val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))

      val getURL = "https://%s/livy/batches/%s".format(inputOptions(Symbol(LivyClientArgumentKeys.ClusterFQDN)), jobId)

      var livyJobDetails: LivyJobDetails = null

      do {
        livyJobDetails = LivyBatchJobAction.get(getURL, jobId, userCredentials(inputOptions), testMode)
        if (livyJobDetails != null) LivyClientUtilities.printJobDetails(livyJobDetails)
        Thread.sleep(1000)
      }
      while (!livyJobDetails.state.equalsIgnoreCase("FINISHED")
        && !livyJobDetails.state.equalsIgnoreCase("ERROR")
        && !livyJobDetails.state.equalsIgnoreCase("UNDEFINED"))
    }
  }

  def kill(inputOptions: LivyClientArgumentParser.ArgumentMap): Unit = {

    val jobId = inputOptions(Symbol(LivyClientArgumentKeys.JobId)).asInstanceOf[Int]

    val deleteURL = "https://%s/livy/batches/%s".format(inputOptions(Symbol(LivyClientArgumentKeys.ClusterFQDN)), jobId)

    val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))

    val livyJobDetails: LivyJobDetails = LivyBatchJobAction.kill(deleteURL, jobId,
      userCredentials(inputOptions), testMode)

    if (livyJobDetails != null) LivyClientUtilities.printJobDetails(livyJobDetails)
  }

  private def userCredentials(inputOptions: LivyClientArgumentParser.ArgumentMap) : LivyUserCredentials = {
    new LivyUserCredentials(inputOptions(Symbol(LivyClientArgumentKeys.ClusterUsername)).toString,
      inputOptions(Symbol(LivyClientArgumentKeys.ClusterPassword)).toString)
  }
}


