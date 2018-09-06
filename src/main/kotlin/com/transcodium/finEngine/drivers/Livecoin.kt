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
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.client.WebClientOptions
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.CoroutineVerticle

class Livecoin  : CoroutineVerticle()  {

    val logger by lazy {
        LoggerFactory.getLogger(this::class.java)
    }


    val driverName = "livecoin"
    lateinit var driverConfig : JsonObject

    lateinit var driverId: String

    val webClient by lazy{

        val webclientOpts = WebClientOptions()
                .setFollowRedirects(true)
                .setTrustAll(true)

        WebClient.create(vertx,webclientOpts)
    }


    override suspend fun start() {
        super.start()

        driverConfig = config.getJsonObject(driverName)

        //lets get config
        val driverInfoStatus = Market.getInfo(driverName,true)

        if(driverInfoStatus.isError()){
            logger.fatalExit(driverInfoStatus.getMessage())
        }

        println(driverInfoStatus.data())

        val driverInfo = driverInfoStatus.data() as JsonObject

        driverId = driverInfo.getString("_id")


        fetchTickerData()
    }//end fun


    /**
     * fetchTickerData
     */
     fun fetchTickerData(){

        //lets get endpoin
        val endpoint =  driverConfig.getString("data_endpoint","")

        if(endpoint.isEmpty()){
            logger.fatal("Livecoin Data endpoint is required in /config/drivers/livecoin.conf")
            return
        }

        val delay = (driverConfig.getInteger("delay",30) * 1000).toLong()


        val httpRequest = webClient.getAbs(endpoint)

        //poll request
        vertx.setPeriodic(delay){

            httpRequest.send{res->

                if(res.failed()){
                    logger.fatal(res.cause().message,res.cause())
                    return@send
                }

                val dataJson = res.result().bodyAsJsonArray()

                processMarketData(dataJson)
            }

        }//end peroidic

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

            val pair =    dataObj.getString("symbol").toLowerCase()
                                        .replace("/",".")


            val priceHigh = dataObj.getDouble("high",0.0)

            val priceLow = dataObj.getDouble("low",0.0)

            val priceClose =  dataObj.getDouble("last",0.0)

            val volume =  dataObj.getDouble("volume",0.0)


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

        DataPiper.save(processedData)

    }//end fun


}//end class