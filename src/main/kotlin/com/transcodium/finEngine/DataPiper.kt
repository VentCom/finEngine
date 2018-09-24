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


import com.mongodb.client.model.InsertOneModel
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import org.bson.Document
import org.bson.types.ObjectId
import java.net.URLDecoder
import java.net.URLEncoder
import java.time.Instant

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

        val collection by lazy {
            MongoDB.client().getCollection("asset_stats")
        }

        /**
         * save - Save the data to a file
         */
        fun save(data: JsonArray){

            val filename = "${System.nanoTime()}.json"

            val filePath = "$dataDir/$filename"

            val charset =  Charsets.UTF_8.name()

            val dataStr = URLEncoder.encode(data.encode(),charset)

            val dataBuff = Buffer.buffer(dataStr,charset)

            fs.writeFile(filePath,dataBuff){res->
                if (res.failed()){
                    logger.fatal("DataPiper file write failed: ${res.cause().message}",res.cause())
                    return@writeFile
                }
            }//end write
        }//end fun


        /**
         * processSaveToDB
         */
        suspend fun processSavedToDB(){

           val scannedFiles: MutableList<String>? = awaitEvent { h ->

               fs.readDir(dataDir) { res ->

                   if (res.failed()) {
                       logger.fatal("Failed to read saved data in $dataDir")
                   }

                   h.handle(res.result())
               }
           }//end await

           if(scannedFiles == null || scannedFiles.isEmpty()){
               return
           }

            //scan through files and insert them
            for (filePath in scannedFiles) {

                   delay(500L)

                    fs.readFile(filePath) { res ->

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

                            //lets update the time millis to mongo date time
                            val datetime = data.getLong(StatItem.TIME)

                            val mongoDate = mongoDate(Instant.ofEpochMilli(datetime))

                            data.put(StatItem.TIME,mongoDate)

                            val docData =  Document.parse(data.encode())

                            InsertOneModel(docData)
                        }.toMutableList()


                        launch(vertxInst().dispatcher()) {

                            delay(500L)

                            //insert into db
                            collection.bulkWrite(mongoBulkData){ res, t ->

                                if(t != null){
                                    logger.fatal("Mongo error:",t)
                                    return@bulkWrite
                                }

                                //delete the file
                                fs.delete(filePath, {})

                            }//end mongo bulk write
                        }

                    }//end read file

                    //delay(2000L)

                }//end loop
        }//end fun

    }//end companion obj



}//end fun