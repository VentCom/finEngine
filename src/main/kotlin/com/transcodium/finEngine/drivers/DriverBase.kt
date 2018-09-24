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

package com.transcodium.finEngine.drivers

import io.vertx.core.AsyncResult
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.kotlin.coroutines.CoroutineVerticle

open class DriverBase : CoroutineVerticle() {

    val logger by lazy {
        LoggerFactory.getLogger(this::class.java)
    }


    lateinit var driverName: String
    lateinit var driverConfig: JsonObject

    val webClient by lazy{

        val webclientOpts = WebClientOptions()
                .setFollowRedirects(true)
                .setTrustAll(true)

        WebClient.create(vertx,webclientOpts)
    }

    override suspend fun start() {
        super.start()

        driverName = config.getString("driver_name")
        driverConfig = config.getJsonObject(driverName)

    }//end fun



    /**
     * fetchTickerData

    fun fetchTickerData(){

        //lets get endpoin
        val endpoint =  driverConfig.getString("data_endpoint","")

        if(endpoint.isEmpty()){
            logger.fatal("$driverName Data endpoint is required in /config/drivers/livecoin.conf")
            return
        }

        val delay = (driverConfig.getInteger("delay",30) * 1000).toLong()


        val httpRequest = webClient.getAbs(endpoint)

        //imediate start
        httpRequest.send{resp-> onHttpResult(resp)}

        //poll request
        vertx.setPeriodic(delay){ httpRequest.send{resp-> onHttpResult(resp) } }//end peroidic

    }//end fun



     * onHttpResult

    private fun onHttpResult(resp: AsyncResult<HttpResponse<Buffer>>){

        if(resp.failed()){
            logger.fatal(resp.cause().message,resp.cause())
            return
        }

        val dataJson = resp.result().bodyAsJsonArray()

        processMarketData(dataJson)
    }//end fun
    */

}