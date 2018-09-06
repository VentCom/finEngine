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
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import io.vertx.kotlin.coroutines.awaitEvent

class Market {

    companion object {

        val logger by lazy{
            LoggerFactory.getLogger(this::class.java)
        }

        val db by lazy {
            DB.client()
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

            //if here, then the data is null
            val insertNew = new(json{obj("name" to name.toLowerCase())})

            if(insertNew.isError()){
                return insertNew
            }

            val insertedId = insertNew.data() as String

            return Status.success( data = json{obj(
                    "_id" to insertedId,
                    "name" to name
            )})

        }//end if

        /**
         * fetchByname
         */
        suspend fun fetchByName(name: String): Status{

            val cond = json{
                obj("name" to name.toLowerCase())
            }

            return awaitEvent { h->
                db.findOne("markets",cond,null){res->

                    if(res.failed()){
                        handleDBError(res,h)
                        return@findOne
                    }

                    val result: JsonObject? = res.result()

                    h.handle(Status.success(data = result))
                }//end
            }//end await
        }

        /**
         * new
         */
        suspend fun new(data: JsonObject): Status{

            return awaitEvent {h->

                db.insert("markets",data){res->

                    if(res.failed()){
                        handleDBError(res,h)
                        return@insert
                    }

                    h.handle(Status.success(data = res.result()))
                } //end insert
            }
        }//end fun

    }//end companion obj
}