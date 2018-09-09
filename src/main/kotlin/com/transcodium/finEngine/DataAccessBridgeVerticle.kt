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

import io.vertx.core.http.HttpServerOptions
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonArray
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.experimental.launch
import io.vertx.ext.mongo.FindOptions


class DataAccessBridgeVerticle: CoroutineVerticle() {

    val logger by lazy {
        LoggerFactory.getLogger(this::class.java)
    }

    val appConfig by lazy {
        config.getJsonObject("app")
    }

    val mClient by lazy {
        DB.client()
    }

    override suspend fun start() {
        super.start()

        startWebsocketServer()
    }//end fun

    /**
     * start websocket server
     */
    fun startWebsocketServer(){

        val port = appConfig.getInteger("data_access_port",9000)
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
    fun webSocketServerHandler(ws: ServerWebSocket){

        val dataAccessAPIKey = appConfig.getString("data_access_api_key","")

        //lets read data
        ws.handler { h->

           val data = try{

               h.toJsonObject()

           }catch(e: Exception){

               ws.writeTextMessage(Status.error("A valid json data is required")
                       .toJsonString()
               )

               //ws.close()

               return@handler
           }

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

            //pairs
            val symbols = data.getJsonArray("symbols",null)

            if(symbols == null || symbols.isEmpty){
                ws.writeTextMessage(Status.error("at least one symbol is required").toJsonString())
                return@handler
            }

            //validate symbols
            for(symbol in symbols){

                symbol as String

                if(!symbol.matches("^([a-zA-Z0-9]+\\.[a-zA-Z0-9]+)$".toRegex())){
                    ws.writeTextMessage(Status.error("Invalid symbol format $symbol").toJsonString())
                    return@handler
                }
            }//end loop



           val timer = vertx.setPeriodic(3000L) {
                launch(vertx.dispatcher()) {
                  ws.writeTextMessage(fecthStatsBySymbols(symbols).toJsonString())
                }
            }//end setPeriodic

            ws.closeHandler {
               vertx.cancelTimer(timer)
            }

        }//end handler
    }//end fun


    /**
     * fetch stats by symbols
     */
    suspend fun fecthStatsBySymbols(
            symbols: JsonArray
    ): Status {

        //since aggregate function is broken for vertx mongo
        //driver, we will use other means to achieve sorting and others
        //we need

        //now lets  fetch the data
        val cond = json{
            obj("s" to obj("\$in" to symbols))
        }

        val findOpts = FindOptions()
                .setSort(json { obj("t" to -1, "v" to -1, "p.x" to -1) })



        return awaitEvent { h->

            mClient.findWithOptions(
                    "asset_stats",cond, findOpts
            ){res->

                if(res.failed()){
                    return@findWithOptions handleDBError(res,h)
                }

                h.handle(Status.success(data = res.result()))
            }
        }

    }//end fun

}//end class