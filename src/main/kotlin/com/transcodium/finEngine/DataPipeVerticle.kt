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

import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.experimental.launch

class DataPipeVerticle : CoroutineVerticle() {

    val scanPeriod = 6000L

    /**
     * start Verticle
     */
    override suspend fun start() {
        super.start()

       // print("data piper verticle started")

        //run this periodically

        vertx.setPeriodic(scanPeriod){
           launch(vertx.dispatcher()) {
               //process data piper data to db
               DataPiper.processSavedToDB()
           }
        }

    }//end fun


}