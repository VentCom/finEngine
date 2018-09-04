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

import io.vertx.core.Vertx
import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.mongo.MongoClient

class DB private constructor(){


    companion object {

        val logger by lazy {
            LoggerFactory.getLogger(this::class.java)
        }

        private val mClient: MongoClient by lazy {
            connect()!!
        }//end

        /**
         * get Client
         */
        fun client(): MongoClient{
            return mClient!!
        }//end


        /**
         * connect
         */
        fun connect()  : MongoClient? {


            val vertx = Vertx.currentContext().owner()

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


            val connection = MongoClient.createShared(vertx, mongoConfig)

            //info
            logger.info("Connected to Mongo DB")

            return connection

        }//end fun

    }//end companion

}
