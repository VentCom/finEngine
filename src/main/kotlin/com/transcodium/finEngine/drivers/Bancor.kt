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


        fetchTickerData()
    }//end


    /**
     * fetchTickerData
     */
    suspend fun fetchTickerData(){

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

            val splitPair = pair.toString().split(".")

            if(splitPair.size < 2){
                logger.fatal("Invalid ticker pair format for $pair for $driverName driver")
                return@forEach
            }

            val coin = splitPair[0].toUpperCase()

            val market = splitPair[1].toUpperCase()

            val url = endpoint.replace(":coinCode:",coin)
                                     .replace(":marketCode:",market)


            val httpRequest = webClient.getAbs(url)

            //immediate start
            httpRequest.send{resp-> onHttpResult(resp, pair) }

            //poll request
            vertx.setPeriodic(delay){ httpRequest.send{resp-> onHttpResult(resp,pair) } }//end peroidic

            //delay for 3 seconds between requests
           Thread.sleep(3000)
        }//end loop


    }//end fun


    /**
     * onHttpResult
     */
    private fun onHttpResult(resp: AsyncResult<HttpResponse<Buffer>>,pair: String){

        if(resp.failed()){
            logger.fatal(resp.cause().message,resp.cause())
            return
        }

        val dataJson = resp.result().bodyAsJsonObject()

        //println(dataJson)
        processMarketData(dataJson,pair)
    }//end fun


    /**
     * processData
     */
    fun processMarketData(httpDataObj: JsonObject,pair: String){

       val dataObj = httpDataObj.getJsonObject("data",null)

        if(dataObj == null){

            println(httpDataObj.encodePrettily())
            logger.fatal("$driverName returned an empty result")
            return
        }

        val processedData = JsonArray()

        val eventTime = System.currentTimeMillis()


         val priceHigh = dataObj.getDouble("price",0.0)

         val priceLow = dataObj.getDouble("price",0.0)

         val priceClose =  dataObj.getDouble("price",0.0)

         val volume =  dataObj.getString("volume24h","0.0").toDouble()


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

        DataPiper.save(processedData)

    }//end fun


}//end class