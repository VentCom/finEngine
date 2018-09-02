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

import com.transcodium.mothership.core.ConfigManager
import io.vertx.core.DeploymentOptions
import io.vertx.core.VertxOptions
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.json.get
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitEvent
import kotlinx.coroutines.experimental.launch

class AppMain : CoroutineVerticle(){

    companion object {

        @JvmStatic
        fun main(args: Array<String>){
            startMainVerticle()
        }

        val logger by lazy {
            LoggerFactory.getLogger(this::class.java)
        }

        /**
         * start main verticle
         */
         fun startMainVerticle() {

             ConfigManager.retrive()
                        .getConfig { res ->

               if(res.failed()) {
                   logger.fatal(res.cause().message,res.cause())
                   return@getConfig
               }

                val config = res.result()


                val appConfig = config.getJsonObject("app")

                val workerPoolSize = appConfig.getInteger("worker_pool_size",3)

                val appInstances = appConfig.getInteger("app_instances",5)

                 val deployOpts = DeploymentOptions()
                         .setConfig(config)
                         .setWorker(true)
                         .setWorkerPoolSize(workerPoolSize)
                         .setInstances(appInstances)
                         .setHa(true)

                            println(config)

             }//end retrieve config

        }//en fun


    }//end companion obj


    /**
     * main Verticle
     */
    override suspend fun start() {
        super.start()

        AppMain.startMainVerticle()
    }
}