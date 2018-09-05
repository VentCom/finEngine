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

package com.transcodium.finEngine


import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.mongo.BulkOperation
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import java.net.URLDecoder
import java.net.URLEncoder

class DataPiper {

    companion object {

        val vertx by lazy {
            vertxInst()
        }

        val fs by lazy {
            vertx.fileSystem()
        }

        val dataDir by lazy{
            "${basePath()}/data_piper"
        }

        val logger by lazy{
            LoggerFactory.getLogger(this::class.java)
        }

        val mClient by lazy {
            DB.client()
        }

        /**
         * save - Save the data to a file
         */
        fun save(data: JsonArray){

            val filename = "${System.nanoTime()}-${data.hashCode()}.json"

            val filePath = "$dataDir/$filename"

            val charset =  Charsets.UTF_8.name()

            val dataStr = URLEncoder.encode(data.encode(),charset)

            val dataBuff = Buffer.buffer(dataStr,charset)

            fs.writeFile(filePath,dataBuff,{res->
                if (res.failed()){
                    logger.fatal("DataPiper file write failed: ${res.cause().message}",res.cause())
                    return@writeFile
                }
            })
        }//end fun


        /**
         * processSaveToDB
         */
        suspend fun processSavedToDB(){

           val scannedFiles: MutableList<String>? = awaitEvent { h ->

               fs.readDir(dataDir, { res ->

                   if (res.failed()) {
                       logger.fatal("Failed to read saved data in $dataDir")
                   }

                   h.handle(res.result())
               })
           }//end await

           if(scannedFiles == null || scannedFiles.isEmpty()){
               return
           }

            //scan through files and insert them
            for (filePath in scannedFiles) {

                   delay(500L)

                    fs.readFile(filePath, { res ->

                        if (res.failed()) {
                            return@readFile
                        }

                        val result: Buffer? = res.result() ?: return@readFile


                        val dataStr = String(result!!.bytes)

                        val charset = Charsets.UTF_8.name()

                        //url decode str
                        val urlDecodedStr = try {
                            URLDecoder.decode(dataStr, charset)
                        } catch (e: Exception) {
                            return@readFile
                        }

                        val dataJsonArray = JsonArray(urlDecodedStr)

                        /**
                         * convert objects to bulkOperations for onetime insert
                         */
                        val mongoBulkData = dataJsonArray.map { data ->

                            data as JsonObject

                            val cond = json {
                                obj(
                                        StatItem.PAIR to data.getString(StatItem.PAIR),
                                        StatItem.MARKET_ID to data.getString(StatItem.MARKET_ID)
                                )
                            }

                            //println(cond)

                            BulkOperation.createReplace(cond, data, true)
                        }.toMutableList()

                        launch(vertxInst().dispatcher()) {

                            delay(500L)

                            //insert into db
                            mClient.bulkWrite("asset_stats", mongoBulkData, { res ->
                                if (res.failed()) {
                                    logger.fatal(
                                            "Failed to insert Bulk Data: ${res.cause().message}",
                                            res.cause()
                                    )
                                    return@bulkWrite
                                }


                                //delete the file
                                fs.delete(filePath, {})

                            })//end mongo bulk write
                        }

                    })//end read file

                    //delay(2000L)

                }//end loop
        }//end fun

    }//end companion obj



}//end fun