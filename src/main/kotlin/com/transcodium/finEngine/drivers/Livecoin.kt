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

class Livecoin  : DriverBase()  {


    override suspend fun start() {
        super.start()
        fetchTickerData()
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
                        StatItem.MARKET_ID to driverName.toLowerCase(),
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