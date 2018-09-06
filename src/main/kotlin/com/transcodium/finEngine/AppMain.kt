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
import io.vertx.core.Vertx
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.coroutines.CoroutineVerticle


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

            //vertx
            val vertx = Vertx.vertx()


            ConfigManager.retrive(vertx)
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
                         .setInstances(1)
                         .setHa(true)

                val defaultDeployOpts = DeploymentOptions()
                                .setConfig(config)
                                .setWorker(true)
                                .setWorkerPoolSize(workerPoolSize)
                                .setInstances(appInstances)
                                .setHa(true)

                 //dataPiperOpts.instances = appInstances

                 //start pipeliner verticler
                 vertx.deployVerticle(DataPipeVerticle::class.java,defaultDeployOpts)

                 //start data access bridge
                 vertx.deployVerticle(DataAccessBridgeVerticle::class.java,defaultDeployOpts)



                            val driversArray = config.getJsonArray("fin_engine_drivers")


                 //loop data and start verticles
                 driversArray.forEach { driverName ->

                     driverName as String

                    //lets get the driver info
                    val driverInfo = config.getJsonObject(driverName)

                     val verticle = driverInfo.getString("verticle") ?: driverName

                     val verticleClass = "${this.javaClass.`package`.name}.drivers.$verticle"

                     //print(verticleClass)

                     //lets start the verticle
                     vertx.deployVerticle(verticleClass,deployOpts){ res->

                         if(res.failed()){
                             logger.fatalExit("Starting Driver verticle $verticleClass failed",res.cause())
                         }else{
                            logger.info("$driverName Started successfully")
                         }
                     }

                 } //end loop

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