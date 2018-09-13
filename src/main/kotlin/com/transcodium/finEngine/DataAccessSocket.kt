/**
# Copyright 2018 - Transcodium Ltd.
#  All rights reserved. This program and the accompanying materials
#  are made available under the terms of the  Apache License v2.0 which accompanies this distribution.
#
#  The Apache License v2.0 is available at
#  http://www.opensource.org/licenses/apache2.0.php
#
#  You are required to redistribute this code under the same licenses.
#
#  @author Razak Zakari <razak@transcodium.com>
#  https://transcodium.com
 **/

package com.transcodium.finEngine

import com.mongodb.client.model.*
import com.mongodb.client.model.Aggregates.*
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonArray
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.experimental.launch
import org.bson.Document
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZoneOffset


class DataAccessSocket: CoroutineVerticle() {

    val logger by lazy {
        LoggerFactory.getLogger(this::class.java)
    }

    val appConfig by lazy {
        config.getJsonObject("app")
    }

    override suspend fun start() {
        super.start()

        startWebsocketServer()
    }//end fun

    /**
     * start websocket server
     */
    suspend fun startWebsocketServer(){

       // StatsData.aggregate()

        val port = appConfig.getInteger("data_access_socket_port",9000)
        val address = appConfig.getString("data_access_address","localhost")

        val serverOpts = HttpServerOptions()
                .setReusePort(true)


        val sockServer = vertx.createHttpServer(serverOpts)
                .websocketHandler { req->  webSocketServerHandler(req)}
                .listen(port,address){  res->

                    if(res.failed()){
                        logger.fatal("Failed to start websocket server $address:$port")
                        return@listen
                    }
                }

    }//end fun


    /**
     * websocket server handler
     */
    fun webSocketServerHandler(ws: ServerWebSocket) {

        val dataAccessAPIKey = appConfig.getString("data_access_api_key", "")

        ws.handler { h ->

            val data = try {

                h.toJsonObject()

            } catch (e: Exception) {

                ws.writeTextMessage(Status.error("A valid json data is required")
                        .toJsonString()
                )

                return@handler
            }//end data


            if(!dataAccessAPIKey.isEmpty()) {

                val apiKey = data.getString("api_key", "")

                if (apiKey.isEmpty()) {
                    ws.writeTextMessage(Status.error("API key required").toJsonString())
                    //ws.reject()
                    return@handler
                }

                if(dataAccessAPIKey != apiKey){
                    ws.writeTextMessage(Status.error("Invalid API Key").toJsonString())
                    //ws.reject()
                    return@handler
                }//end if

            }//end if server api key was set


            //symbols
            val symbols: JsonArray? = data.getJsonArray("symbols",null)

            val period = data.getString("period","hourly")

            //coroutines
            launch(vertx.dispatcher()) {

                val statsDataStatus = StatsData.aggregate(null, period)

                println(statsDataStatus)
            }//end coroutines

        }//end handler


    }//end fun



}//end class