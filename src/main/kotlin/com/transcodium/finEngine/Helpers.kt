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

import com.transcodium.mothership.core.StatusCodes
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.core.logging.Logger
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.core.json.get
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj
import java.io.File
import java.time.Instant
import kotlin.reflect.KClass


val logger by lazy {
    LoggerFactory.getLogger("Helpers.kt")
}


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


/**
 * log fatal and exit process
 */
fun Logger.fatalExit(str: String, errInfo: Any? = null){

    var message = str

    var throwable: Throwable? = null

    var exception: Exception? = null

    if(errInfo is AsyncResult<*>){

        throwable = errInfo.cause()

        exception = (throwable as Exception)

        message += "\r\n Cause: ${throwable.cause}"

        println(message)

    }else if(errInfo is Throwable){

        throwable = errInfo

    }

    this.fatal(message,throwable)

    if(errInfo is Exception){
        throw errInfo
    }

    throw Exception(throwable)
}


/**
 * containKeys
 * check a jsonObject if a set of keys exists, if the non existent key will be
 * sent back
 */

fun JsonObject.requiredItems(keys: List<String>): String? {

    val jsonData = this

    keys.forEach {
        key ->

        if(!jsonData.containsKey(key) || jsonData.getValue(key).toString() == ""){
            return key
        }
    }

    return null
}//end fun



/**
 *handleDBError
 **/
fun handleDBError(
        asyncCallback: AsyncResult<*>,
        h: Handler<Status>? = null){

    if(asyncCallback.failed()){

        if(h != null){
            h.handle(Status.error(
                    message = "system_busy",
                    code = StatusCodes.DB_ERROR
            ))
        }

        logger.fatalExit("Mongo DB Error Occured",asyncCallback.cause())
    }//end if
}//end fun

/**
 * vertxInst
 **/
fun vertxInst(): Vertx{
    return Vertx.currentContext().owner()
}

/**
 *  makeDir
 **/
suspend fun makeDir(dirPath: String){

}//end


/**
 *currentDate
 **/
fun currentDateTime(): Instant {
    return Instant.now()
}//end

/**
 *mongoDate
 **/
fun mongoDate(customDate: Instant? = null): JsonObject{

    val date = customDate ?: currentDateTime()

    return json{
        obj("\$date" to date.toString())
    }
}