import com.mongodb.client.model.Aggregates
import com.transcodium.finEngine.MongoDB
import com.transcodium.finEngine.Status
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
                symbols: MutableList<String>? = null,
                interval: String? = "hourly"
        ): Status {


            var groupDate: Document

            val now = LocalDateTime.now(ZoneId.of("UTC"))

            var dateStart: LocalDateTime
            var dateEnd : LocalDateTime = now

            when(interval){

                "hourly" -> {
                    groupDate = Document()
                            .append("hour", Document("\$hour","\$t"))

                    //stats for last 12 hours
                    dateStart =  now.minusHours(12)
                }

                "daily" -> {
                    groupDate = Document()
                            .append("day", Document("\$dayOfMonth","\$t"))
                            .append("month", Document("\$month","\$t"))
                            .append("year", Document("\$year","\$t"))

                    // a 7 days interval
                    dateStart = now.minusDays(7)
                }

                "mothly" -> {
                    groupDate = Document()
                            .append("month", Document("\$month","\$t"))
                            .append("year", Document("\$year","\$t"))

                    //since last 3 months
                    dateStart = now.minusMonths(3)
                }
                else -> {
                    return Status.error("Unknown date interval")
                }
            }//end when

            val queryDateStart = dateStart.toInstant(ZoneOffset.UTC)
            val queryDateEnd = dateEnd.toInstant(ZoneOffset.UTC)


            ///match
            val matchCond = Document("\$match",
                    Document("t", Document()
                            .append("\$gte", queryDateStart)
                            .append("\$lte",queryDateEnd)
                    )
            )//end match

            //if symbols was provided
            if(symbols != null){
                matchCond.append("s", Document("\$in",symbols))
            }

            //sort by first
            //1. Latest Date Time
            //2. Volume
            //3. Price ( closed price p.x)
            val sortCmd = Aggregates.sort(
                    Document()
                            .append("t",1)
                            .append("v",1)
                            .append("p.x",1)
            )

            val groupCmd = Document("\$group",
                    Document("_id", groupDate
                            .append("symbol","\$s")
                            .append("price","\$p")
                            .append("volume", Document("\$sum","\$v"))
                    )
            )

            //not aggregates must be in order else you
            // will get undesirable results
            val aggregateCmd = mutableListOf(
                    matchCond,
                    sortCmd,
                    groupCmd
            )


            val resultStatus = awaitEvent<Status> { h->

                val resultDocs = mutableListOf<Document>()


                mClient.getCollection("asset_stats")
                        .aggregate(aggregateCmd)
                        .allowDiskUse(true)
                        .into(resultDocs) { _, t ->

                            if (t != null) {
                                t.printStackTrace()
                                return@into h.handle(Status.error("db_error"))
                            }

                            /**
                            resultDocs.forEach { i ->
                            println(i)
                            }
                             */

                            h.handle(Status.success(data = resultDocs))
                        }
            }//end await


            return Status.success(data = resultStatus)
        }//end fun


    }
}