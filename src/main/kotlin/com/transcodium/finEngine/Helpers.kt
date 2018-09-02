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
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.json.get
import org.jetbrains.exposed.sql.Database
import java.io.File
import kotlin.reflect.KClass


val logger by lazy {
    LoggerFactory.getLogger("Helpers.kt")
}

/**
 * DBConnect
 */
fun DB(): Database{

    val dbConfig = getConfig<JsonObject?>("database",null)

    if(dbConfig== null){
        logger.fatal("Database config is missing")
        System.exit(1)
    }

    val dbType = dbConfig!!.getString("type")
    val host = dbConfig.getString("host")
    val port = dbConfig.getInteger("port")
    val database = dbConfig.getInteger("database")

    val dbUrl = "jdbc:$dbType://$host:$port/$database"

    return Database.connect(
            url = dbUrl,
            driver =  dbConfig.getString("driver"),
            user = dbConfig.getString("user"),
            password =  dbConfig.getString("password")
    )
}//end

/**
 * getConfig
 */
fun <T>getConfig(key: String, default: T): T {

    val config = Vertx.currentContext()
                                 .config() ?: return default

    return config.get<T>(key) ?: return default
}//end fun


/**
 *basePath - App's Root Directory
 *@param callerClass
 * @return File Object
 **/
fun basePath(callerClass: KClass<*>? = null): File {

    val clazz = callerClass ?: AppMain::class

    var appDir: String = System.getProperty("app.dir","")

    //print(appDir)

    if(appDir.isEmpty()) {

        //get current Jar path
        appDir = clazz.java.protectionDomain.codeSource.location.path

        if(appDir.endsWith(".jar")){
            appDir = File(appDir).parent
        }

        System.setProperty("app.dir",appDir)
    }

    // print(appDir)

    return File(appDir)
}//end