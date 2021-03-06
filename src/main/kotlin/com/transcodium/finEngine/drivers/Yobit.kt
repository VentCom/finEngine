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
import com.transcodium.finEngine.StatItem
import io.vertx.core.AsyncResult
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj


class Yobit : DriverBase(){


    override suspend fun start() {
        super.start()
        fetchTickerData()
    }//end fun

    /**
     * fetchTickerData
     */
    fun fetchTickerData(){

        //lets get endpoin
        val endpoint =  driverConfig.getString("data_endpoint","")

        if(endpoint.isEmpty()){
            logger.fatal("$driverName Data endpoint is required in /config/drivers/livecoin.conf")
            return
        }

        val delay = (driverConfig.getInteger("delay",30) * 1000).toLong()

        val tickerPairs = driverConfig.getJsonArray("pairs",null)

        if(tickerPairs == null){
            logger.fatal("$driverName Ticker pairs are required")
            return
        }

        val concatPairs = tickerPairs.joinToString("-")

        val url = "$endpoint/$concatPairs"

        val httpRequest = webClient.getAbs(url)
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

        val dataJson = resp.result().bodyAsJsonObject()

        processMarketData(dataJson)
    }//end fun


    /**
     * processData
     */
    fun processMarketData(allDataObj: JsonObject){

        if(allDataObj.isEmpty){
            return
        }

        val processedData = JsonArray()


        for((pairStr,dataObj) in allDataObj){

            dataObj as JsonObject


            val eventTime = System.currentTimeMillis()

            val pair =    pairStr.toLowerCase()
                                        .replace("_",".")


            val priceHigh = dataObj.getDouble("high",0.0)

            val priceLow = dataObj.getDouble("low",0.0)

            val priceClose =  dataObj.getDouble("last",0.0)

            val volume =  dataObj.getDouble("vol",0.0)


            processedData.add(json{
                obj(
                        StatItem.TIME  to eventTime,
                        StatItem.PAIR to pair,
                        StatItem.MARKET_ID to driverName.toLowerCase(),
                        StatItem.PRICE to obj(
                                StatItem.PRICE_LOW  to priceLow,
                                StatItem.PRICE_HIGH  to priceHigh,
                                StatItem.PRICE_CLOSE to priceClose
                        ),

                        StatItem.VOLUME to volume
                )
            })

        }//end loop

        //println(processedData)

        DataPiper.save(processedData)

    }//end fun


}//end

