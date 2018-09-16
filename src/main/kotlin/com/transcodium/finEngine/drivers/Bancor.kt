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

import com.transcodium.finEngine.Market
import com.transcodium.finEngine.fatalExit
import io.vertx.core.AsyncResult
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.kotlin.coroutines.CoroutineVerticle
import org.bson.Document

class Bancor : CoroutineVerticle() {

    val logger by lazy {
        LoggerFactory.getLogger(this::class.java)
    }

    val driverName = "bancor"
    var driverId : String? = null
    var driverConfig: JsonObject? = null

    val webClient by lazy{

        val webclientOpts = WebClientOptions()
                .setFollowRedirects(true)
                .setTrustAll(true)

        WebClient.create(vertx,webclientOpts)
    }

    /**
     * start
     */
    override suspend fun start(){

        //lets get config
        val driverInfoStatus = Market.getInfo(driverName,true)

        if(driverInfoStatus.isError()){
            logger.fatal(driverInfoStatus.getMessage())
            System.exit(1)
        }

        val driverInfo = driverInfoStatus.getData<Document>()!!

        driverId = try{
            driverInfo.getString("_id")
        }catch(e: Exception){
            driverInfo.getObjectId("_id").toHexString()
        }


        driverConfig = config.getJsonObject(driverName)

        if(driverConfig == null){
            logger.fatalExit("Config file for $driverName not found")
            return
        }



    }//end


    /**
     * fetchTickerData
     */
    fun fetchTickerData(){

        //lets get endpoin
        val endpoint =  driverConfig!!.getString("data_endpoint","")

        if(endpoint.isEmpty()){
            logger.fatal("$driverName Data endpoint is required in /config/drivers/$driverName.conf")
            return
        }

        val delay = (driverConfig!!.getInteger("delay",30) * 1000).toLong()

        //lets get tickers
        val tickersArray = driverConfig!!.getJsonArray("tickers",null)

        //if tickersArray is empty or null
        if(tickersArray == null || tickersArray.isEmpty){
            logger.fatal("$driverName tickers pair array is required or empty")
            return
        }


        //loop the ticker pair
        tickersArray.forEach { pair ->

            pair as String

            val splitPair = pair.toString().split("_")

            if(splitPair.size < 2){
                logger.fatal("Invalid ticker pair format for $pair for $driverName driver")
                return@forEach
            }

            val coin = splitPair[0]

            val market = splitPair[1]

            val url = "$endpoint"

            val httpRequest = webClient.getAbs(endpoint)

            //immediate start
            httpRequest.send{resp-> onHttpResult(resp) }

            //poll request
            vertx.setPeriodic(delay){ httpRequest.send{resp-> onHttpResult(resp) } }//end peroidic

        }//end loop


    }//end fun


    /**
     * onHttpResult
     */
    private fun onHttpResult(resp: AsyncResult<HttpResponse<Buffer>>){

        if(resp.failed()){
            logger.fatal(resp.cause().message,resp.cause())
            return
        }

        val dataJson = resp.result().bodyAsJsonArray()


        //processMarketData(dataJson)
    }//end fun

}