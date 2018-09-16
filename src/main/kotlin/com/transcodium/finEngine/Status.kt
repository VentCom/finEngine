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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.transcodium.mothership.core.StatusCodes
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.get

/**
 * Status after a function call
 */
 object Status{


       private var isError: Boolean = false
       private var isSuccess: Boolean = false
       private var isNeutral: Boolean = false
       private var message: String = ""
       private var type: String = ""
       private var data : Any? = null
       private var isSevere = false
       private var killProcess = false

       private var code = 0


        fun instance(): Status{
            return this
        }

        /**
         * success
         */
        fun success(message: String="",
                    data: Any? = null,
                    code: Int? = null): Status{

            this.isError = false

            this.isSuccess = true

            this.message = message

            this.type = "success"

            this.data = data

            this.code = code ?: StatusCodes.SUCCESS

            return this
        }

        /**
         * success
         */
        fun neutral(message: String="",
                    data: Any? = null,
                    code: Int? = null): Status{

            this.isError = false

            this.isSuccess = false

            this.isNeutral = true

            this.message = message

            this.type = "success"

            this.data = data

            this.code = code ?: StatusCodes.NEUTRAL

            return this

        }//end fun


        /**
        * messageless success
        */
        fun success(data: Any? = null): Status{

            this.code = StatusCodes.SUCCESS

            success("",data)
            return this
        }


        /**
         * error
         */
        fun error(
                message: String= "",
                data: Any? = null,
                code: Int? = null,
                isSevere: Boolean? = false
        ): Status{

            this.isError = true

            this.isSuccess = false

            this.isNeutral = false

            this.type = "error"

            this.message = message

            this.data = data

            this.code = code ?: StatusCodes.FAILED

            this.isSevere = isSevere!!

            return this
        }



        /**
        * set
        */
        fun set(type: String,
                message: String = "",
                data: Any? = null,
                code: Int? = null
            ): Status{

            return when(type){
               "success" -> this.success(message,data,code)
                "error"  -> this.error(message,data,code)
                else     -> this.neutral(message,data,code)
            }

        } //end fun



        //isError
        fun isError(): Boolean{
            return this.isError
        }

        //succeeded
        fun isSuccess(): Boolean{
            return this.isSuccess
        }

        fun isNeutral(): Boolean{
            return this.isNeutral
        }

        //get message
        fun message(): String{
            return this.message
        }//end

        /**
        * getMessage
        */
        fun getMessage(): String {
            return this.message
        }

        /**
         * setMessage
         */
        fun setMessage(message: String): Status{
           this.message = message
            return instance()
        }

        /**
        * code
        */
        fun code(): Int {
            return this.code
        }

        /**
        * set code
        */
        fun setCode(code: Int): Status{
            this.code = code
            return instance()
        }

        /**
         * data
         */
        fun data(): Any?{
            return this.data
        }

        fun <T>getData(): T?{
            return this.data as T
        }

        /**
        * setData
        */
        fun <T> setData(data: T): Status{
            this.data = data
            return instance()
        }


         fun fromJson(data:JsonObject): Status {

            val alertType = data.getString("type")

            if(alertType == "error"){
                return this.error(
                        data.getString("message",""),
                         data.get<Any>("data"),
                         data.getInteger("code",0)

                )
            }else if(alertType == "success") {

                return this.success(
                        data.getString("message",""),
                        data.get<Any>("data"),
                        code
                )

            }else{

                return this.neutral(
                        data.getString("message",""),
                        data.get<Any>("data"),
                        code
                )

            }//end if

        }//end

        /*
        *toJson
         */
        fun toJsonString(): String {
            return toJsonObject().toString()
        }

        /**
         * to Json Object
         */
       fun toJsonObject(): JsonObject {
           return JsonObject(mapOf(
                   "type" to type,
                   "message" to message,
                   "code" to code,
                   "data" to data,
                   "is_severe"  to isSevere
           ))
       }//end

}//end class


