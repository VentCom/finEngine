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
import io.vertx.kotlin.core.json.JsonObject
import io.vertx.kotlin.coroutines.awaitBlocking
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.ext.mongo.BulkOperation
import kotlinx.coroutines.experimental.delay

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
        fun save(data: Any){

            val filename = "${System.nanoTime()}-${data.hashCode()}.json"

            val filePath = "$dataDir/$filename"

            val dataBuff = Buffer.buffer(data.toString())

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
        suspend fun processSavedToDB(infinite: Boolean = false){

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
            scannedFiles.forEach { filePath->

                fs.readFile(filePath,{ res->

                    if(res.failed()){
                        logger.fatal(
                            "Failed to read contents of $filePath : ${res.cause().message}",
                                res.cause()
                        )
                    }

                    val result: Buffer? = res.result()

                    val dataJsonArray = JsonArray(String(result!!.bytes))

                    /**
                     * convert objects to bulkOperations for onetime insert
                     */
                    val mongoBulkData = dataJsonArray.map{ data ->
                        BulkOperation.createInsert(data as JsonObject)
                    }.toMutableList()


                    //insert into db
                    mClient.bulkWrite("asset_stats",mongoBulkData,{
                        res->
                        if(res.failed()){
                            logger.fatal(
                                    "Failed to insert Bulk Data: ${res.cause().message}",
                                    res.cause()
                            )
                            return@bulkWrite
                        }


                        //delete the file
                        fs.deleteBlocking(filePath)

                    })//end mongo bulk write

                })//end read file

                //delay(2000L)

            }//end loop

            ///if infinite
            if(infinite) {
                //restart processing again
                processSavedToDB(infinite)
            }//end if

        }//end fun

    }
}