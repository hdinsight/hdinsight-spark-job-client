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

import com.microsoft.livy.client.common._
import com.microsoft.livy.client.trust.manager.LivyClientTrustProvider
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicCredentialsProvider, CloseableHttpClient}
import org.json4s.ShortTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization._

object LivyBatchJobAction {

  def list(listUrl: String, userDetails: LivyUserCredentials, testMode: Boolean): LivySessionList = {

    val credentialsProvider:  CredentialsProvider = new BasicCredentialsProvider()
    val userCredentials: UsernamePasswordCredentials = new UsernamePasswordCredentials(userDetails.userName,
      userDetails.userPassword)
    credentialsProvider.setCredentials(AuthScope.ANY, userCredentials)

    val httpClient: CloseableHttpClient = LivyClientTrustProvider.getHttpClient(credentialsProvider, testMode)

    val httpResponse = httpClient.execute(new HttpGet(listUrl))
    val statusCode: Int = httpResponse.getStatusLine().getStatusCode()

    val responseEntity = httpResponse.getEntity()
    var responseContent = ""

    if (responseEntity != null) {

      val inputStream = responseEntity.getContent()
      responseContent = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }

    httpClient.close()

    if(statusCode == 200 || statusCode == 201) {

      //return new LivyMessage(responseContent)

      implicit val formats = Serialization.formats(
        ShortTypeHints(
          List()
        )
      )

      return read[LivySessionList](responseContent)
    }

    return new LivySessionList(-1, -1, List[LivySession]())
  }

  def get(getUrl: String, jobId: Long, userDetails: LivyUserCredentials, testMode: Boolean): LivyJobDetails = {

    val credentialsProvider:  CredentialsProvider = new BasicCredentialsProvider()
    val userCredentials: UsernamePasswordCredentials = new UsernamePasswordCredentials(userDetails.userName,
      userDetails.userPassword)
    credentialsProvider.setCredentials(AuthScope.ANY, userCredentials)

    val httpClient: CloseableHttpClient = LivyClientTrustProvider.getHttpClient(credentialsProvider, testMode)

    val httpResponse = httpClient.execute(new HttpGet(getUrl))
    val statusCode: Int = httpResponse.getStatusLine().getStatusCode()

    val responseEntity = httpResponse.getEntity()
    var responseContent = ""

    if (responseEntity != null) {

      val inputStream = responseEntity.getContent()
      responseContent = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }

    httpClient.close()

    if(statusCode == 200 || statusCode == 201) {

      implicit val formats = Serialization.formats(
        ShortTypeHints(
          List()
        )
      )

      return read[LivyJobDetails](responseContent)
    }

    return new LivyJobDetails(jobId, "UNDEFINED", List[String](responseContent))
  }

  def submit(postUrl: String, userDetails: LivyUserCredentials,  jobRequest: LivyBatchJob, testMode: Boolean): LivyJobDetails = {

    implicit val formats = Serialization.formats(
      ShortTypeHints(
        List()
      )
    )

    val prettyJobRequestJSON = writePretty(jobRequest)

    println(s"Serialized Job Request = $prettyJobRequestJSON")

    val credentialsProvider:  CredentialsProvider = new BasicCredentialsProvider()
    val userCredentials: UsernamePasswordCredentials = new UsernamePasswordCredentials(userDetails.userName,
      userDetails.userPassword)
    credentialsProvider.setCredentials(AuthScope.ANY, userCredentials)

    val postRequest = new HttpPost(postUrl)
    postRequest.addHeader("Content-Type", "application/json")
    postRequest.setEntity(new StringEntity(write(jobRequest)))

    val httpClient: CloseableHttpClient = LivyClientTrustProvider.getHttpClient(credentialsProvider, testMode)

    val httpResponse = httpClient.execute(postRequest)
    val statusCode: Int = httpResponse.getStatusLine().getStatusCode()

    val responseEntity = httpResponse.getEntity()

    var responseContent: String = ""

    if (responseEntity != null) {

      val inputStream = responseEntity.getContent()
      responseContent = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }

    httpClient.close()

    if(statusCode == 200 || statusCode == 201) {

      return read[LivyJobDetails](responseContent)
    }

    return new LivyJobDetails(-1, "UNDEFINED", List[String](responseContent))
  }

  def kill(deleteUrl: String, jobId: Long, userDetails: LivyUserCredentials, testMode: Boolean): LivyJobDetails = {

    val credentialsProvider:  CredentialsProvider = new BasicCredentialsProvider()
    val userCredentials: UsernamePasswordCredentials = new UsernamePasswordCredentials(userDetails.userName,
      userDetails.userPassword)
    credentialsProvider.setCredentials(AuthScope.ANY, userCredentials)

    val httpClient: CloseableHttpClient = LivyClientTrustProvider.getHttpClient(credentialsProvider, testMode)

    val httpResponse = httpClient.execute(new HttpDelete(deleteUrl))
    val statusCode: Int = httpResponse.getStatusLine().getStatusCode()

    val responseEntity = httpResponse.getEntity()
    var responseContent: String = ""

    if (responseEntity != null) {

      val inputStream = responseEntity.getContent()
      responseContent = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }

    httpClient.close()

    if(statusCode == 200 || statusCode == 201) {

      implicit val formats = Serialization.formats(
        ShortTypeHints(
          List()
        )
      )

      val livyMessage: LivyMessage = read[LivyMessage](responseContent)

      return new LivyJobDetails(jobId, "KILLED", List[String](livyMessage.msg))

    }
    else if(statusCode == 404) {

      return new LivyJobDetails(jobId, "NOTFOUND", List[String](responseContent))
    }

    return new LivyJobDetails(jobId, "UNDEFINED", List[String](responseContent))
  }
}
