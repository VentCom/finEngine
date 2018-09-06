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

import io.vertx.core.logging.LoggerFactory
import io.vertx.ext.bridge.BridgeOptions
import io.vertx.ext.bridge.PermittedOptions
import io.vertx.ext.eventbus.bridge.tcp.TcpEventBusBridge
import io.vertx.kotlin.coroutines.CoroutineVerticle

class DataAccessBridgeVerticle: CoroutineVerticle() {

    val logger by lazy {
        LoggerFactory.getLogger(this::class.java)
    }

    override suspend fun start() {
        super.start()

        val bridge = TcpEventBusBridge.create(
                vertx,
         BridgeOptions()
                .addInboundPermitted( PermittedOptions().setAddress("in"))
                .addOutboundPermitted( PermittedOptions().setAddress("out")))

        bridge.listen(9000){res->

            if(res.failed()){
                logger.fatalExit(
                        "Data Access Bridge failed ${res.cause().message}",
                        res.cause()
                )

                bridge.close()

                return@listen
            }

            
        }

    }//end fun

}