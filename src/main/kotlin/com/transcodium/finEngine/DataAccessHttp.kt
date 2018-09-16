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
import io.vertx.core.json.JsonArray
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.experimental.launch

class DataAccessHttp : CoroutineVerticle() {

    override suspend fun start() {
        super.start()

        //start http server
        startHTTPServer()
    }


    val appConfig by lazy {
        config.getJsonObject("app")
    }


    /**
     * initHttpServer
     */
    suspend fun startHTTPServer(){

        val port = appConfig.getInteger("data_access_http_port",8000)
        val address = appConfig.getString("data_access_address","localhost")
        val dataAccessAPIKey = appConfig.getString("data_access_api_key", "")


        //http server options
        val serverOpts = HttpServerOptions()
                .setHost(address)
                .setPort(port)
                .setCompressionSupported(true)
                .setTcpKeepAlive(true)
                .setTcpNoDelay(true)
                .setTcpFastOpen(true)
                .setDecompressionSupported(true)
                .setReuseAddress(true)

        val router = Router.router(vertx)


        //lets check if we have api keys
        if(dataAccessAPIKey != null){
            router.route().handler {ctx->

                val apiKey = ctx.request().getParam("api_key") ?: ""

                if(apiKey.isEmpty()){
                    response(ctx,Status.error("API Key is required"))
                    return@handler
                }

                if(apiKey != dataAccessAPIKey){
                    response(ctx,Status.error("API Key is not valid"))
                    return@handler
                }

                //continue
                ctx.next()
            }//end handler
        }//end if api is required


        //listen to aggregates
        router.get("/aggregate").handler { ctx ->
            launch(vertx.dispatcher()){
                aggregateHandler(ctx)
            }
        }

        vertx.createHttpServer(serverOpts)
                .requestHandler{router.accept(it)}
                .listen { res->
                    if(res.failed()){
                        logger.fatalExit("Http Server Failed: ${res.cause().message}",res.cause())
                        return@listen
                    }

                    logger.info("HTTP Server Started at $address:$port")
                }
    }//end fun

    /**
     * jsonResponse
     */
    fun response(ctx: RoutingContext,status: Status){
        ctx.response()
                .setStatusCode(200)
                .putHeader("Content-Type","application/json")
                .end(status.toJsonString())
    }


    /**
     * aggregate Handler
     */
    suspend fun aggregateHandler(ctx: RoutingContext){

        val request = ctx.request()

        val symbols = request.getParam("symbols") ?: null

        var symbolsList :JsonArray? = null

        if(!symbols.isNullOrEmpty()){
            symbolsList = JsonArray(symbols!!.split(","))
        }

        val interval = request.getParam("intervals") ?: "minute"

        val dataStatus =  StatsData.aggregate(symbolsList,interval)

        response(ctx,dataStatus)
    }//end

}//end class