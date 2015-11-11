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

package com.microsoft.livy.client.argument.parser

object LivyClientArgumentKeys extends Enumeration {
  val ListJobs: String = "listJobs"
  val SubmitJob: String = "submitJob"
  val SubmitAndMonitorJob: String = "submitAndMonitorJob"
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

object LivyClientArgumentParser {

  type ArgumentMap = Map[Symbol, Any]

  def usageExample(): Unit = {

    val jobId: Int = 3
    val applicationName: String = "SparkEventHubStreaming"
    val applicationJAR: String = "/home/hdiuser/spark-streaming/SparkStreamingEventHubsEventCount.jar"
    val applicationClass: String = "com.microsoft.spark.streaming.examples.EventHubsStreaming"
    val applicationArguments: String = "/eventcheckpoint10, <EventHubPolicyName>, <EventHubKey>," +
      " sparkstreamingeventhub-ns, sparkstreamingeventhub, 8, $default, 10, /EventCount/EventCount10"
    val classPathJARS: String = "/usr/hdp/current/spark-client/lib/datanucleus-api-jdo-3.2.6.jar," +
      " /usr/hdp/current/spark-client/lib/datanucleus-rdbms-3.2.9.jar," +
      " /usr/hdp/current/spark-client/lib/datanucleus-core-3.2.10.jar"
    val executorFiles: String = "/usr/hdp/current/spark-client/conf/hive-site.xml"
    val executorCount: Int = 16
    val perExecutorCoreCount: Int = 1
    val perExecutorMemoryInGB: Int = 2
    val driverMemoryInGB: Int = 4
    val clusterFQDN: String = "sparkstreamingonhumboldt06.azurehdinsight.net"
    val clusterUsername: String = "<ClusterUsername>"
    val clusterPassword: String = "<ClusterPassword>"

    println()
    println(s"Usage: --submit|--submit-and-monitor --applicationName $applicationName" +
      s" --application-jar $applicationJAR --application-class $applicationClass" +
      s" --application-arguments  $applicationArguments --jars-in-classpath $classPathJARS" +
      s" --files-in-executor-working-directory $executorFiles --executor-count $executorCount" +
      s" --per-executor-core-count $perExecutorCoreCount --per-executor-memory-in-gb $perExecutorMemoryInGB" +
      s" --driver-memory-in-gb $driverMemoryInGB --cluster-fqdn $clusterFQDN --cluster-username $clusterUsername" +
      s" --cluster-password $clusterPassword --batch-mode|--interactive-mode")
    println()
    println(s"Usage: --monitor|--kill --job-id $jobId --cluster-fqdn $clusterFQDN --cluster-username $clusterUsername" +
      s" --cluster-password $clusterPassword --batch-mode|--interactive-mode")
    println()
    println(s"Usage: --list --cluster-fqdn $clusterFQDN --cluster-username $clusterUsername" +
      s" --cluster-password $clusterPassword --batch-mode|--interactive-mode")
    println()
  }

  def parseArguments(argumentMap : ArgumentMap, argumentList: List[String]) : ArgumentMap = {

    argumentList match {
      case Nil => argumentMap
      case "--list" :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.ListJobs) -> true), tail)
      case "--submit" :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.SubmitJob) -> true), tail)
      case "--submit-and-monitor" :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.SubmitAndMonitorJob) -> true), tail)
      case "--monitor" :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.MonitorJob) -> true), tail)
      case "--kill" :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.KillJob) -> true), tail)
      case "--application-name" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.ApplicationName) -> value.toString), tail)
      case "--application-jar" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.ApplicationJAR) -> value.toString), tail)
      case "--application-class" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.ApplicationClass) -> value.toString), tail)
      case "--application-arguments" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.ApplicationArguments)
          -> value.toString.split(Array(',', ';', ' ')).map(_.trim).toList.filter(_.nonEmpty)), tail)
      case "--jars-in-classpath" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.ClasspathJARS)
          -> value.toString.split(Array(',', ';')).map(_.trim).toList.filter(_.nonEmpty)), tail)
      case "--files-in-executor-working-directory" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.ExecutorFiles)
          -> value.toString.split(Array(',', ';')).map(_.trim).toList.filter(_.nonEmpty)), tail)
      case "--executor-count" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.ExecutorCount) -> value.toInt), tail)
      case "--per-executor-core-count" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.PerExecutorCoreCount) -> value.toInt), tail)
      case "--per-executor-memory-in-gb" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.PerExecutorMemoryInGB) -> value.toInt), tail)
      case "--driver-memory-in-gb" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.DriverMemoryInGB) -> value.toInt), tail)
      case "--cluster-fqdn" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.ClusterFQDN) -> value.toString), tail)
      case "--cluster-username" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.ClusterUsername) -> value.toString), tail)
      case "--cluster-password" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.ClusterPassword) -> value.toString), tail)
      case "--yarn-queue-name" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.YarnQueue) -> value.toString), tail)
      case "--batch-mode" :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.BatchMode) -> true), tail)
      case "--interactive-mode" :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.InteractiveMode) -> true), tail)
      case "--job-id" :: value :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.JobId) -> value.toInt), tail)
      case "--test-mode" :: tail =>
        parseArguments(argumentMap ++ Map(Symbol(LivyClientArgumentKeys.TestMode) -> true), tail)
      case option :: tail =>
        println()
        println("Unknown option: " + option)
        println()
        usageExample()
        sys.exit(1)
    }
  }

  def verifyUpdateArguments(argumentMap : ArgumentMap): Unit = {

    //Remove this when interactive job is supported

    if (argumentMap.contains(Symbol(LivyClientArgumentKeys.InteractiveMode))) {
      println()
      println("Interactive job is not supported yet.")
      println()
      sys.exit(1)
    }

    //Verify cluster and user details have been specified

    assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.ClusterFQDN)))
    assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.ClusterUsername)))
    assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.ClusterPassword)))

    //Verify only one of the job submission modes has been specified

    var modeCount: Int = 0

    if (argumentMap.contains(Symbol(LivyClientArgumentKeys.BatchMode))) {
      modeCount += 1
    }

    if (argumentMap.contains(Symbol(LivyClientArgumentKeys.InteractiveMode))) {
      modeCount += 1
    }

    if (modeCount == 0) {
      println()
      println("Only one job mode must be specified.")
      println()
      usageExample()
      sys.exit(1)
    }

    if (modeCount > 1) {
      println()
      println("Only one job mode can be specified.")
      println()
      usageExample()
      sys.exit()
    }

    //Verify only one of the job actions has been specified

    var actionCount: Int = 0

    if (argumentMap.contains(Symbol(LivyClientArgumentKeys.ListJobs))) {
      actionCount += 1
    }

    if (argumentMap.contains(Symbol(LivyClientArgumentKeys.SubmitJob))) {
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.ApplicationJAR)))
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.ApplicationClass)))
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.ExecutorCount)))
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.PerExecutorCoreCount)))
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.PerExecutorMemoryInGB)))
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.DriverMemoryInGB)))
      actionCount += 1
    }

    if (argumentMap.contains(Symbol(LivyClientArgumentKeys.SubmitAndMonitorJob))) {
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.ApplicationJAR)))
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.ApplicationClass)))
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.ExecutorCount)))
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.PerExecutorCoreCount)))
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.PerExecutorMemoryInGB)))
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.DriverMemoryInGB)))
      actionCount += 1
    }

    if (argumentMap.contains(Symbol(LivyClientArgumentKeys.MonitorJob))) {
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.JobId)))
      actionCount += 1
    }

    if (argumentMap.contains(Symbol(LivyClientArgumentKeys.KillJob))) {
      assert(argumentMap.contains(Symbol(LivyClientArgumentKeys.JobId)))
      actionCount += 1
    }

    if (actionCount == 0) {
      println()
      println("Only one job action must be specified.")
      println()
      usageExample()
      sys.exit(1)
    }

    if (actionCount > 1) {
      println()
      println("Only one job action can be specified.")
      println()
      usageExample()
      sys.exit()
    }
  }
}
