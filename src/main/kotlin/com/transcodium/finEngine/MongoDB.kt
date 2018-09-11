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

import com.mongodb.MongoClientSettings
import com.mongodb.MongoCompressor
import com.mongodb.MongoCredential
import com.mongodb.ServerAddress
import com.mongodb.async.client.MongoClients
import com.mongodb.async.client.MongoDatabase
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import org.bson.Document
import org.bson.types.ObjectId


class MongoDB private constructor(){


    companion object {

        private var mongoClient: MongoDatabase? = null

        val logger by lazy {
            LoggerFactory.getLogger(this::class.java)
        }

        private val mClient: MongoDatabase by lazy {
            connect()
        }//end

        /**
         * get Client
         */
        fun client(): MongoDatabase{
            return mClient
        }//end


        /**
         * connect
         */
        private fun connect()  : MongoDatabase {

            if(mongoClient != null){
               // return mongoClient!!
            }

            //get database config
            val config = Vertx.currentContext().config()


            if (!config.containsKey("database")) {
                logger.fatalExit("Database config was not found")
            }

            //mongo config
            val mongoConfig = config.getJsonObject("database")
                    .getJsonObject("mongodb")


            //lastly, check if the required value exists
            val missingItem = mongoConfig.requiredItems(listOf("hosts", "db_name", "username", "password"))

            if (missingItem is String) {
                logger.fatalExit("The config item '$missingItem' in database.conf is either missing or empty")
            }


            val hostsArray = mongoConfig.getJsonArray("hosts")

            val database =   mongoConfig.getString("db_name")

            val hostLists = hostsArray.map{ hostInfo ->
                hostInfo as JsonObject

                ServerAddress(
                        hostInfo.getString("host"),
                        hostInfo.getInteger("port")
                )

            }.toMutableList()

            val credentials = MongoCredential.createCredential(
                    mongoConfig.getString("username"),
                    database,
                    mongoConfig.getString("password").toCharArray()
            )



            val mongoSettings = MongoClientSettings
                    .builder()
                    .credential(credentials)
                    .applyToClusterSettings{builder ->
                        builder.hosts(hostLists)
                                .build()
                    }
                    .applyToSslSettings{builder->
                        builder.enabled(false)
                                .build()
                    }
                    .applyToSocketSettings{builder ->
                        builder.keepAlive(true)

                    }
                    .compressorList(listOf(
                            MongoCompressor.createSnappyCompressor(),
                            MongoCompressor.createZlibCompressor()
                    ))
                    .build()

            val client = MongoClients.create(mongoSettings)



            mongoClient =  client.getDatabase(database)

            return mongoClient!!
        }//end fun


        /**
         * insert One
         */
        fun insert(
                collection: String,
                data: Document,
                lambda: (result: String, t: Throwable)-> Unit
        ){

            if(!data.containsKey("_id")){
                data.put("_id",ObjectId())
            }

            val objId = data.getObjectId("_id")
                                    .toHexString()

            mClient.getCollection(collection)
                    .insertOne(data){_,t->
                        lambda(objId,t)
                    }

        }//end fun

    }//end companion
}


