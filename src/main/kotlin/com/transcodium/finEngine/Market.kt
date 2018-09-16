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

import com.mongodb.client.model.Filters.eq
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.awaitEvent
import org.bson.Document
import org.bson.types.ObjectId

class Market {

    companion object {

        val logger by lazy{
            LoggerFactory.getLogger(this::class.java)
        }

        val collection by lazy {
            MongoDB.client().getCollection("markets")
        }

        /**
         * getInfo - Get market info by name
         */
         suspend fun getInfo(name: String,
                             insertMissing : Boolean = false): Status {

            var infoStatus = fetchByName(name)

            if(infoStatus.isError() || !insertMissing){
                return infoStatus
            }

            //if the returned data is null, means the market does not exists
            if(infoStatus.data() != null){
                return infoStatus
            }

            val dataToStore = Document().apply {
                put("name",name.toLowerCase())
            }


            //if here, then the data is null
            val insertNew = new(dataToStore)

            if(insertNew.isError()){
                return insertNew
            }

            val insertedId = insertNew.data() as String

            val finalData = Document()
                                        .append("_id",insertedId)
                                        .append("name", name)

            return Status.success( data = finalData)

        }//end if

        /**
         * fetchByname
         */
        suspend fun fetchByName(name: String): Status{

            val cond = eq("name",name.toLowerCase())

            return awaitEvent { h->
                collection.find(cond).first { result, t ->

                    if(t != null){
                        return@first handleDBError(t,h)
                    }


                    h.handle(Status.success(data = result))

                }//end find

            }//end await
        }

        /**
         * new
         */
        suspend fun new(data: Document): Status{

            return awaitEvent { h ->
                MongoDB.insert("markets",data){ id, t->

                    if(t?.cause != null){
                        return@insert handleDBError(t,h)
                    }

                    h.handle(Status.success(data = id))
                }
            }
        }//end fun

    }//end companion obj
}