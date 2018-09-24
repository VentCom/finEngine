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

import com.transcodium.finEngine.*
import com.transcodium.finEngine.StatItem.Companion.PRICE_LOW
import io.vertx.core.AsyncResult
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpClientOptions
import io.vertx.core.http.RequestOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.web.client.HttpResponse
import io.vertx.kotlin.core.json.JsonArray
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.core.net.NetClientOptions
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.experimental.launch
import org.bson.Document


class Binance : CoroutineVerticle() {

    val logger by lazy {
        LoggerFactory.getLogger(this::class.java)
    }

    lateinit var driverName: String
    lateinit var driverConfig: JsonObject

    val wsReconnectWaitTime = 10000L

    /**
     * start
     */
    override suspend fun start(){

        driverName = config.getString("driver_name")

        driverConfig = config.getJsonObject(driverName)

        //start http stream
        fetchMarketHttpDataStream()
    }//end


    /**
     * fetchMarketDataStream
     */
    suspend fun fetchWebSocketMarketDataStream(vertx: Vertx){

        var options = NetClientOptions(
                connectTimeout = 10000,
                ssl = true,
                 trustAll = true
        )

        var httpOpts = HttpClientOptions()
                .setMaxWebsocketFrameSize(100000000)


        val client = vertx.createHttpClient(httpOpts)

        val websocketHost = driverConfig!!.getString("websocket_host","")
        val websocketPort = driverConfig!!.getInteger("websocket_port",0)
        val websocketUri = driverConfig!!.getString("websocket_uri","")

        val socOptions = RequestOptions()
                .setSsl(true)
                .setHost(websocketHost)
                .setPort(websocketPort)
                .setURI(websocketUri)


        val dataPiper = DataPiper

        val soc = client.websocket(socOptions){res->

            res.exceptionHandler {e->
                logger.fatal(e.message,e)
            }

            /**
             * listen to incomming data
             */
            res.handler { h->

                launch(vertx.dispatcher()) {

                    processAndSaveWebSocketMarketStream(h.toJsonArray())

                }//end coroutine
             }//end handler

            /**
             * listen to close handler
             */
            res.closeHandler {

              print("connection closed, waiting for ${wsReconnectWaitTime / 1000}  secs to reconnect")

              vertx.setTimer(wsReconnectWaitTime){
                  launch(vertx.dispatcher()) {
                      fetchWebSocketMarketDataStream(vertx)
                  }
              }//end timer
            }
        }//end websocket


    }//end fun



    /**
     * onHttpResult
     */
    private fun onHttpResult(resp: AsyncResult<HttpResponse<Buffer>>){

        if(resp.failed()){
            logger.fatal(resp.cause().message,resp.cause())
            return
        }

        processHttpMarketData(resp.result().bodyAsJsonArray())
    }//end fun


    /**
     * fetchMarketHttpDataStream
     */
    suspend fun fetchMarketHttpDataStream(){

        //lets get endpoin
        val endpoint =  driverConfig!!.getString("data_endpoint","")

        if(endpoint.isEmpty()){
            logger.fatal("$driverName Data endpoint is required in /config/drivers/livecoin.conf")
            return
        }

        val delay = (driverConfig!!.getInteger("delay",30) * 1000).toLong()

        val webClient =  getWebClient()

        val httpRequest = webClient.getAbs(endpoint)

        //imediate start
        httpRequest.send{resp->onHttpResult(resp)}

        //poll request
        vertx.setPeriodic(delay){ httpRequest.send{resp-> onHttpResult(resp) } }//end peroidic

    }//end fun


    /**
     * processData
     */
    fun processHttpMarketData(dataArray: JsonArray){

        if(dataArray.isEmpty){
            return
        }

        val processedData = JsonArray()


        dataArray.forEach { dataObj ->

            dataObj as JsonObject

            val eventTime = System.currentTimeMillis()

            val pair =    dataObj.getString("symbol").toLowerCase()

            var firstAsset: String
            var secondAsset: String

            if(pair.matches(".*(btc|eth|bnb)$".toRegex())){
                secondAsset = pair.substring(pair.length - 3,pair.length)
                firstAsset = pair.substring(0,(pair.length - secondAsset.length))
            }else{
                firstAsset = pair.substring(0,3)
                secondAsset = pair.substring(3,pair.length)
            }

            val symbol = "$firstAsset.$secondAsset"

            //val priceChange = dataObj.getString("priceChange").toDoubleOrNull()

            //val priceChangePercent = dataObj.getString("P").toFloat()

            val priceHigh = dataObj.getString("highPrice").toDoubleOrNull()

            val priceLow = dataObj.getString("lowPrice").toDoubleOrNull()

            val priceOpen = dataObj.getString("openPrice").toDoubleOrNull()

            val priceClose =  dataObj.getString("lastPrice").toDoubleOrNull()

            val volume =  dataObj.getString("volume").toDoubleOrNull()

            val volumeQoute = dataObj.getString("quoteVolume").toDoubleOrNull()



            processedData.add(json{
                obj(
                        StatItem.TIME  to eventTime,
                        StatItem.PAIR to symbol,
                        StatItem.MARKET_ID to driverName.toLowerCase(),
                        StatItem.PRICE to obj(

                                //StatItem.PRICE_CHANGE to priceChange,
                               // StatItem.PRICE_CHANGE_PERCENT to priceChangePercent,
                                StatItem.PRICE_LOW  to priceLow,
                                StatItem.PRICE_HIGH  to priceHigh,
                                StatItem.PRICE_OPEN  to priceOpen,
                                StatItem.PRICE_CLOSE to priceClose
                        ),

                        StatItem.VOLUME       to volume,
                        StatItem.VOLUME_QUOTE to volumeQoute
                )
            })

        }//end loop

       // println(processedData)

        DataPiper.save(processedData)

    }//end fun



    /**
     * save market stream data
     */
    fun processAndSaveWebSocketMarketStream(dataArray: JsonArray){


        val processedData = JsonArray()

        dataArray.forEach { dataObj->

            dataObj as JsonObject


            val eventTime = dataObj.getLong("E")

            //val dateTimeInstant = Instant.ofEpochMilli(eventTime)

            val pair =    dataObj.getString("s").toLowerCase()

            var firstAsset: String
            var secondAsset: String

            if(pair.matches("^(btc|eth|bnb).+".toRegex())){
                firstAsset = pair.substring(0,3)
                secondAsset = pair.substring(3,pair.length)
            }else{
                secondAsset = pair.substring(pair.length - 3,pair.length)
                firstAsset = pair.substring(0,(pair.length - secondAsset.length))
            }


            val symbol = "$firstAsset.$secondAsset"

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
                        StatItem.TIME  to eventTime,
                        StatItem.PAIR to symbol,
                        StatItem.MARKET_ID to driverName.toLowerCase(),
                        StatItem.PRICE to obj(

                                StatItem.PRICE_CHANGE to priceChange,
                                StatItem.PRICE_CHANGE_PERCENT to priceChangePercent,
                                StatItem.PRICE_LOW  to priceLow,
                                StatItem.PRICE_HIGH  to priceHigh,
                                StatItem.PRICE_OPEN  to priceOpen,
                                StatItem.PRICE_CLOSE to priceClose
                        ),

                        StatItem.VOLUME       to volume,
                        StatItem.VOLUME_QUOTE to volumeQoute
                )
            })
        }//end loop


        DataPiper.save(processedData)
    }//end fun

}//enc class