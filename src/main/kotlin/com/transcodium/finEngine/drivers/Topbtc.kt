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

import com.transcodium.finEngine.DataPiper
import com.transcodium.finEngine.Market
import com.transcodium.finEngine.StatItem
import com.transcodium.finEngine.fatalExit
import io.vertx.core.AsyncResult
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle
import org.bson.Document

class Topbtc : CoroutineVerticle(){

    val logger by lazy {
        LoggerFactory.getLogger(this::class.java)
    }


    val driverName = "topbtc"

    lateinit var driverConfig: JsonObject

    lateinit var driverId: String

    val webClient by lazy {

        val userAgent = "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"

        val webclientOpts = WebClientOptions()
                .setFollowRedirects(true)
                .setTrustAll(true)
                .setUserAgent(userAgent)

        WebClient.create(vertx, webclientOpts)
    }


    override suspend fun start() {
        super.start()

        driverConfig = config.getJsonObject(driverName)

        //lets get config
        val driverInfoStatus = Market.getInfo(driverName, true)

        if (driverInfoStatus.isError()) {
            logger.fatalExit(driverInfoStatus.getMessage())
        }

        //println(driverInfoStatus.data())

        val driverInfo = driverInfoStatus.data() as Document

       // println(driverInfo)

        driverId = try {
            driverInfo.getString("_id")
        } catch (e: Exception) {
            driverInfo.getObjectId("_id").toHexString()
        }


        fetchTickerData()
    }//end fun


    /**
     * fetchTickerData
     */
    fun fetchTickerData(){

        //lets get endpoin
        val endpoint =  driverConfig.getString("data_endpoint","")

        if(endpoint.isEmpty()){
            logger.fatal("$driverName Data endpoint is required in /config/drivers/$driverName.conf")
            return
        }

        val delay = (driverConfig.getInteger("delay",30) * 1000).toLong()


        val httpRequest = webClient.getAbs(endpoint)
                .putHeader("Accept","application/json, text/javascript, */*; q=0.01")
                .putHeader("Content-Type","application/x-www-form-urlencoded")

        //immediate start
        httpRequest.send{resp-> onHttpResult(resp) }

        //poll request
        vertx.setPeriodic(delay){ httpRequest.send{resp-> onHttpResult(resp) } }//end peroidic

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


        processMarketData(dataJson)
    }//end fun


    /**
     * processData
     */
    fun processMarketData(dataArray: JsonArray){

        if(dataArray.isEmpty){
            return
        }

        val processedData = JsonArray()


        dataArray.forEach { dataObj ->

            dataObj as JsonObject

            val eventTime = System.currentTimeMillis()

            val coin =    dataObj.getString("coin").toLowerCase()

            val market =  dataObj.getString("market").toLowerCase()

            val pair = "$coin.$market"

            val tickerInfoObj = dataObj.getJsonObject("ticker")

            val priceHigh = tickerInfoObj.getString("high","0.0").toDouble()

            val priceLow = tickerInfoObj.getString("low","0.0").toDouble()

            val priceClose =  tickerInfoObj.getString("last","0.0").toDouble()

            val volume =  tickerInfoObj.getString("vol","0.0").toDouble()


            processedData.add(json{
                obj(
                        StatItem.TIME  to eventTime,
                        StatItem.PAIR to pair,
                        StatItem.MARKET_ID to driverId,
                        StatItem.PRICE to obj(
                                StatItem.PRICE_LOW  to priceLow,
                                StatItem.PRICE_HIGH  to priceHigh,
                                StatItem.PRICE_CLOSE to priceClose
                        ),

                        StatItem.VOLUME       to volume
                )
            })
        }//end loop


        //println(processedData)

        DataPiper.save(processedData)

    }//end fun



}//end class