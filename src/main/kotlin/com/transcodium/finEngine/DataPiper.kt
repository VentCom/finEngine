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

import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.json.Json
import io.vertx.kotlin.coroutines.awaitEvent

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

        /**
         * save - Save the data to a file
         */
        fun save(data: JsonObject){

            val filename = "${System.nanoTime()}-${data.hashCode()}.json"

            val filePath = "$dataDir/$filename"

            fs.writeFile(filePath,data.toBuffer(),{res->
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

            val processedFiles : JsonObject = JsonObject()

            //scan through files and insert them
            scannedFiles.forEach { filePath->

                val proessFile = awaitEvent<JsonObject>{h->
                    fs.readFile(filePath,{ res->

                        if(res.failed()){
                            logger.fatal(
                                "Failed to read contents of $filePath : ${res.cause().message}",
                                    res.cause()
                            )
                        }

                    })
                }
            }//end


        }//end fun

    }
}