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
import org.bson.Document

class Hitbtc: DriverBase() {

    override suspend fun start() {
        super.start()
        fetchTickerData()
    }

    fun fetchTickerData(){

        //lets get endpoin
        val endpoint =  driverConfig!!.getString("data_endpoint","")

        if(endpoint.isEmpty()){
            logger.fatal("Hitbtc Data endpoint is required in /config/drivers/hitbtc.conf")
            return
        }

        val delay = (driverConfig!!.getInteger("delay",30) * 1000).toLong()


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

    fun processMarketData(dataArray: JsonArray){

        if(dataArray.isEmpty){
            return
        }

        val processedData = JsonArray()


        for(dataObj in dataArray) {

            dataObj as JsonObject

            if(dataObj.isEmpty){
                continue
            }

            val eventTime = System.currentTimeMillis()

            val pair =    dataObj.getString("symbol").toLowerCase()

            var firstAsset: String
            var secondAsset: String


           if(pair.matches(".*(btc|eth|eos|usd|dai)$".toRegex())){
                secondAsset = pair.substring(pair.length - 3,pair.length)
                firstAsset = pair.substring(0,(pair.length - secondAsset.length))
            }

            else if(pair.length == 6 || pair.matches("^(btc|eth|eos|dai).+".toRegex())){

               firstAsset = pair.substring(0, 3)
               secondAsset = pair.substring(3, pair.length)

           }

            else if( pair.matches(".+(usdt|eurs|tusd)$".toRegex())){

               secondAsset = pair.substring(pair.length - 4,pair.length)
               firstAsset = pair.substring(0,(pair.length - secondAsset.length))

            }else {

               firstAsset = pair.substring(0,4)
               secondAsset = pair.substring(4,pair.length)

           }

            val symbol = "$firstAsset.$secondAsset"

            //println(symbol)

            val priceHigh = dataObj.getString("high","0.0").toDoubleOrNull()

            val priceLow = dataObj.getString("low","0.0").toDoubleOrNull()

            val priceOpen = dataObj.getString("open","0.0")?.toDoubleOrNull() ?: 0.0

            val priceClose =  dataObj.getString("last","0.0").toDoubleOrNull()

            val volume =  dataObj.getString("volume","0.0").toDoubleOrNull()

            val volumeQoute = dataObj.getString("volumeQuote","0.0").toDoubleOrNull()



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

}