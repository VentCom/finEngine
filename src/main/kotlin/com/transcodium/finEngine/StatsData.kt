package com.transcodium.finEngine

import com.mongodb.client.model.Aggregates
import io.vertx.core.json.JsonArray
import io.vertx.kotlin.coroutines.awaitEvent
import org.bson.Document
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset

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

class StatsData {

    companion object {


        val mClient by lazy {
            MongoDB.client()
        }

        /**
         * fetch data
         */
        suspend fun aggregate(
                symbols: JsonArray? = null,
                interval: String? = "hourly",
                since: Long? = null
        ): Status {


            var groupDate: Document

            val now = LocalDateTime.now(ZoneId.of("UTC"))

            var dateStart: LocalDateTime
            var dateEnd : LocalDateTime = now

            val statSince = since ?: 1

            when(interval){

                "minute" -> {

                    groupDate = Document()
                                    .append("year",Document("\$year","\$t"))
                                    .append("month",Document("\$month","\$t"))
                                    .append("day",Document("\$dayOfMonth","\$t"))
                                    .append("hour",Document("\$hour","\$t"))
                                    .append("minute",Document("\$minute","\$t"))

                    //stats for last 60minutes
                    dateStart =  now.minusMinutes(statSince)
                }

                "hourly" -> {
                    groupDate = Document()
                                    .append("year",Document("\$year","\$t"))
                                    .append("month",Document("\$month","\$t"))
                                    .append("day",Document("\$dayOfMonth","\$t"))
                                    .append("hour",Document("\$hour","\$t"))


                    //stats for last 3 hours
                    dateStart =  now.minusHours(statSince)
                }

                "daily" -> {
                    groupDate = Document()
                                    .append("year",Document("\$year","\$t"))
                                    .append("month",Document("\$month","\$t"))
                                    .append("day",Document("\$dayOfMonth","\$t"))

                    // a 7 days interval
                    dateStart = now.minusDays(statSince)
                }

                "mothly" -> {
                    groupDate = Document()
                                    .append("year",Document("\$year","\$t"))
                                    .append("month",Document("\$month","\$t"))

                    //since last 3 months
                    dateStart = now.minusMonths(statSince)
                }
                else -> {
                    return Status.error("Unknown date interval")
                }
            }//end when

            val queryDateStart = dateStart.toInstant(ZoneOffset.UTC)
            val queryDateEnd = dateEnd.toInstant(ZoneOffset.UTC)


            ///match
            val matchCond = Document("t", Document()
                            .append("\$gte", queryDateStart)
                            .append("\$lte",queryDateEnd)
                    )


            //if symbols was provided
            if(symbols != null){
                matchCond.append("s", Document("\$in",symbols))
            }

            //match
            val aggregateMatch = Document("\$match",matchCond)


            //pre sort
            val preSort = Aggregates.sort(
                                Document()
                                    //.append("t",-1)
                                     .append("v",-1)
                                     .append("p.x",-1)
            )

            //sort by first
            //1. Latest Date Time
            val postSort = Aggregates.sort(
                                    Document()
                                        .append("_id.date",-1)
            )

            //id is date + symbol
            val groupCmd = Document("\$group", Document()
                                        .append("_id", Document("date",groupDate)
                                        .append("symbol","\$s"))
                                        .append("price",Document("\$first","\$p"))
            )

            //not aggregates must be in order else you
            // will get undesirable results
            val aggregateCmd = mutableListOf(
                    aggregateMatch,
                    preSort,
                    groupCmd,
                    postSort
            )


            return awaitEvent{ h->

                val resultDocs = mutableListOf<Document>()


                mClient.getCollection("asset_stats")
                        .aggregate(aggregateCmd)
                        .allowDiskUse(true)
                        .into(resultDocs) { _, t ->

                            if (t != null) {
                                t.printStackTrace()
                                return@into h.handle(Status.error("db_error"))
                            }


                            h.handle(Status.success(data = resultDocs))
                        }
            }//end await

        }//end fun


    }
}