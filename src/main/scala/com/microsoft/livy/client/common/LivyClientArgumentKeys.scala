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

package com.microsoft.livy.client.common

object LivyClientArgumentKeys extends Enumeration {
  val ListJobs: String = "listJobs"
  val SubmitJob: String = "submitJob"
  val RunJob: String = "runJob"
  val MonitorJob: String = "monitorJob"
  val KillJob: String = "killJob"
  val JobId: String = "jobId"
  val ApplicationName: String = "applicationName"
  val ApplicationJAR: String = "applicationJAR"
  val ApplicationClass: String = "applicationClass"
  val ApplicationArguments: String = "applicationArguments"
  val ExecutorFiles: String = "executorFiles"
  val ClasspathJARS: String = "classpathJARS"
  val ExecutorCount: String = "executorCount"
  val PerExecutorCoreCount: String = "perExecutorCoreCount"
  val PerExecutorMemoryInGB: String = "perExecutorMemoryInGB"
  val DriverMemoryInGB: String = "driverMemoryInGB"
  val ClusterFQDN: String = "clusterFQDN"
  val ClusterUsername: String = "clusterUsername"
  val ClusterPassword: String = "clusterPassword"
  val YarnQueue: String = "yarnQueue"
  val BatchMode: String = "batchMode"
  val InteractiveMode: String = "interactiveMode"
  val TestMode: String = "testMode"
}