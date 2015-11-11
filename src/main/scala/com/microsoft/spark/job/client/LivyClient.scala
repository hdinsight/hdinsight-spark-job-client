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

package com.microsoft.spark.job.client

import com.microsoft.livy.client.argument.parser.{LivyClientArgumentParser, LivyClientArgumentKeys}
import com.microsoft.livy.client.batch.job.{LivyBatchJobAction, LivyBatchJob}
import com.microsoft.livy.client.common._

object LivyClient {

  def main(inputArguments: Array[String]) {

    val inputOptions = LivyClientArgumentParser.parseArguments(Map(), inputArguments.toList)

    LivyClientArgumentParser.verifyUpdateArguments(inputOptions)

    //println(inputOptions)

    val testMode: Boolean = inputOptions.contains(Symbol(LivyClientArgumentKeys.TestMode))

    val userDetails = new LivyUserCredentials(inputOptions(Symbol(LivyClientArgumentKeys.ClusterUsername)).toString(),
      inputOptions(Symbol(LivyClientArgumentKeys.ClusterPassword)).toString())

    // List jobs

    if (inputOptions.contains(Symbol(LivyClientArgumentKeys.ListJobs))) {

      val listURL = "https://%s/livy/batches".format(inputOptions(Symbol(LivyClientArgumentKeys.ClusterFQDN)))

      val livySessionList: LivySessionList = LivyBatchJobAction.list(listURL, userDetails, testMode)

      if (livySessionList != null) {
        LivyClientUtilities.printSessionList(livySessionList)
      }
    }

    // Submit job

    var jobId: Long = -1

    if (inputOptions.contains(Symbol(LivyClientArgumentKeys.SubmitJob))
      || inputOptions.contains(Symbol(LivyClientArgumentKeys.SubmitAndMonitorJob))) {

      val jobRequest: LivyBatchJob = new LivyBatchJob(if (inputOptions.contains(Symbol(LivyClientArgumentKeys.ApplicationName))) {
        inputOptions(Symbol(LivyClientArgumentKeys.ApplicationName)).toString()
      } else {
        "Livy"
      },
        inputOptions(Symbol(LivyClientArgumentKeys.ApplicationJAR)).toString(),
        inputOptions(Symbol(LivyClientArgumentKeys.ApplicationClass)).toString(),
        inputOptions(Symbol(LivyClientArgumentKeys.ApplicationArguments)).asInstanceOf[List[String]],
        inputOptions(Symbol(LivyClientArgumentKeys.ExecutorCount)).asInstanceOf[Int],
        inputOptions(Symbol(LivyClientArgumentKeys.PerExecutorCoreCount)).asInstanceOf[Int],
        inputOptions(Symbol(LivyClientArgumentKeys.PerExecutorMemoryInGB)).toString() + "G",
        inputOptions(Symbol(LivyClientArgumentKeys.DriverMemoryInGB)).toString() + "G",
        if (inputOptions.contains(Symbol(LivyClientArgumentKeys.ClasspathJARS))) {
          inputOptions(Symbol(LivyClientArgumentKeys.ClasspathJARS)).asInstanceOf[List[String]]
        } else {
          List[String]()
        },
        if (inputOptions.contains(Symbol(LivyClientArgumentKeys.ExecutorFiles))) {
          inputOptions(Symbol(LivyClientArgumentKeys.ExecutorFiles)).asInstanceOf[List[String]]
        } else {
          List[String]()
        })

      val postURL = "https://%s/livy/batches".format(inputOptions(Symbol(LivyClientArgumentKeys.ClusterFQDN)))

      val livyJobDetails: LivyJobDetails = LivyBatchJobAction.submit(postURL, userDetails, jobRequest, testMode)

      if (livyJobDetails != null) {
        LivyClientUtilities.printJobDetails(livyJobDetails)
      }

      jobId = livyJobDetails.id
    }

    // Monitor job

    if (inputOptions.contains(Symbol(LivyClientArgumentKeys.SubmitAndMonitorJob))
      || inputOptions.contains(Symbol(LivyClientArgumentKeys.MonitorJob))) {

      if (jobId == -1) {
        jobId = inputOptions(Symbol(LivyClientArgumentKeys.JobId)).asInstanceOf[Int]
      }

      val getURL = "https://%s/livy/batches/%s".format(inputOptions(Symbol(LivyClientArgumentKeys.ClusterFQDN)), jobId)

      var livyJobDetails: LivyJobDetails = null

      do {
        livyJobDetails = LivyBatchJobAction.get(getURL, jobId, userDetails, testMode)

        if (livyJobDetails != null) {
          LivyClientUtilities.printJobDetails(livyJobDetails)
        }

        //Sleep for 1 second

        Thread.sleep(1000)
      }
      while (!livyJobDetails.state.equalsIgnoreCase("FINISHED")
        && !livyJobDetails.state.equalsIgnoreCase("ERROR")
        && !livyJobDetails.state.equalsIgnoreCase("UNDEFINED"))
    }

    // Kill job

    if (inputOptions.contains(Symbol(LivyClientArgumentKeys.KillJob))) {

      jobId = inputOptions(Symbol(LivyClientArgumentKeys.JobId)).asInstanceOf[Int]

      val deleteURL = "https://%s/livy/batches/%s".format(inputOptions(Symbol(LivyClientArgumentKeys.ClusterFQDN)), jobId)

      val livyJobDetails: LivyJobDetails = LivyBatchJobAction.kill(deleteURL, jobId, userDetails, testMode)

      if (livyJobDetails != null) {
        LivyClientUtilities.printJobDetails(livyJobDetails)
      }
    }
  }
}

