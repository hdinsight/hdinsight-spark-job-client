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

package com.microsoft.livy.client.trust.manager

import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl._

import org.apache.http.client.CredentialsProvider
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}

class LivyClientTrustManager extends X509TrustManager {

  override def checkClientTrusted(authorizationCertificate: Array[X509Certificate], authorizationType: String): Unit = {
  }

  override def checkServerTrusted(authorizationCertificate: Array[X509Certificate], authorizationType: String) {
  }

  override def getAcceptedIssuers = null
}


class LivyClientHostnameVerifier extends HostnameVerifier {

  override def verify(hostname: String, sslSession: SSLSession): Boolean = {

    return true
  }
}

object LivyClientTrustProvider {

  def getHttpClient(credentialsProvider: CredentialsProvider, testMode: Boolean): CloseableHttpClient = {

    //Trusts any certificate for use in test mode only

    if(testMode == true) {

      return getOpenHttpClient(credentialsProvider)
    }

    return HttpClientBuilder.create().setDefaultCredentialsProvider(credentialsProvider).build()
  }


  private def getOpenHttpClient(credentialsProvider: CredentialsProvider): CloseableHttpClient = {

    val openTrustManager = Array[TrustManager](new LivyClientTrustManager())
    val openHostNameVerifier: HostnameVerifier =  new LivyClientHostnameVerifier

    val sslContext: SSLContext = SSLContext.getInstance("TLS")

    sslContext.init(null, openTrustManager, new SecureRandom())

    val socketFactory: SSLConnectionSocketFactory  = new SSLConnectionSocketFactory(sslContext, openHostNameVerifier)

    val httpClient: CloseableHttpClient = HttpClientBuilder.create().setSSLSocketFactory(socketFactory)
      .setDefaultCredentialsProvider(credentialsProvider).build();

    return httpClient;
  }
}

