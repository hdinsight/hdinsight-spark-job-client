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

object LivyClientUtilities {

  def printSessionList(livySessionList: LivySessionList): Unit = {

    println()
    println("Starting Id: " + livySessionList.from)
    println("Total Sessions: " + livySessionList.total)
    println()
    livySessionList.sessions.foreach(livySession => {
      println("Session Id: " + livySession.id)
      println("Session State: " + livySession.state)
      println("Session Logs: ")
      livySession.log.foreach {
        println
      }
      println()
    })
  }

  def printJobDetails(livyJobDetails: LivyJobDetails): Unit = {

    println()
    println("Jod ID: " + livyJobDetails.id)
    println("Jod State: " + livyJobDetails.state)
    println("Job Logs: ")
    livyJobDetails.log.foreach {
      println
    }
    println()
  }

  def printMessage(livyMessage: LivyMessage): Unit = {
    println()
    println("Message: ")
    println(livyMessage.msg)
    println()
  }
}
