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
import com.transcodium.finEngine.fatalExit
import com.transcodium.finEngine.mongoDate
import io.vertx.core.Vertx
import io.vertx.core.http.HttpClient
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.RequestOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.json.Json
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.core.net.NetClientOptions
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.experimental.launch
import java.time.Instant

class Binance : CoroutineVerticle() {

    val logger by lazy {
        LoggerFactory.getLogger(this::class.java)
    }

    val driverName = "binance"
    var driverId : String? = null
    var driverConfig: JsonObject? = null

    val wsReconnectWaitTime = 10000L

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

        val driverInfo = driverInfoStatus.data() as JsonObject

        driverId = driverInfo.getString("_id")


        driverConfig = config.getJsonObject(driverName)

        if(driverConfig == null){
            logger.fatalExit("Config file for $driverName not found")
            return
        }


        fetchMarketDataStream(vertx)
    }//end


    /**
     * getLatestPrices
     */
    fun fetchLatestPrices(){


    }//end

    /**
     * fetchMarketDataStream
     */
    suspend fun fetchMarketDataStream(vertx: Vertx){

        var options = NetClientOptions(
                connectTimeout = 10000,
                ssl = true,
                 trustAll = true
        )

        var httpOpts = HttpClientOptions()
                .setMaxWebsocketFrameSize(100000000)


        val client = vertx.createHttpClient(httpOpts)

        val socOptions = RequestOptions()
                .setSsl(true)
                .setHost("stream.binance.com")
                .setPort(9443)
                .setURI("/ws/!ticker@arr")


        val dataPiper = DataPiper

        val soc = client.websocket(socOptions,{res->

            res.exceptionHandler {e->
                logger.fatal(e.message,e)
            }

            /**
             * listen to incomming data
             */
            res.handler { h->

                launch(vertx.dispatcher()) {

                    processAndSaveMarketStream(h.toJsonArray())

                }//end coroutine
             }//end handler

            /**
             * listen to close handler
             */
            res.closeHandler {

              print("connection closed, waiting for ${wsReconnectWaitTime / 1000}  secs to reconnect")

              vertx.setTimer(wsReconnectWaitTime,{
                  launch(vertx.dispatcher()) {
                      fetchMarketDataStream(vertx)
                  }
              })
            }
        })


    }//end fun

    /**
     * save market stream data
     */
    fun processAndSaveMarketStream(dataArray: JsonArray){

        val processedData = JsonArray()

        dataArray.forEach { dataObj->

            dataObj as JsonObject

            //println(dataObj)

            val eventTime = dataObj.getLong("E")

            //val dateTimeInstant = Instant.ofEpochMilli(eventTime)

            val pair =    dataObj.getString("s").toLowerCase()

            val priceChange = dataObj.getString("p").toDoubleOrNull()

            val priceChangePercent = dataObj.getString("P").toFloat()

            val priceHigh = dataObj.getString("h").toDoubleOrNull()

            val priceLow = dataObj.getString("l").toDoubleOrNull()

            val priceOpen = dataObj.getString("o").toDoubleOrNull()

            val priceClose =  dataObj.getString("c").toDoubleOrNull()

            val volume =  dataObj.getString("v").toDoubleOrNull()

            val volumeQoute = dataObj.getString("q").toDoubleOrNull()


            processedData.add(json{
                obj(
                        "t"  to eventTime,
                        "s" to pair,
                        "mid" to driverId,
                        "p" to obj(
                                "c" to priceChange,
                                "cp" to priceChangePercent,
                                "l"  to priceLow,
                                "h"  to priceHigh,
                                "o"  to priceOpen,
                                "cl" to priceClose
                        ),

                        "v" to volume,
                        "vq" to volumeQoute
                )
            })
        }//end loop

        DataPiper.save(processedData)
    }//end fun

}//enc class