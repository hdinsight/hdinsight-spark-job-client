package com.microsoft.livy.client.common

class LivySession(
                   var id: Long,
                   var state: String,
                   var log: List[String])

class LivySessionList(
                       var from: Long,
                       var total: Long,
                       var sessions: List[LivySession])
