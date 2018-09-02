package com.transcodium.mothership.core


import com.transcodium.finEngine.basePath
import io.vertx.config.ConfigRetriever
import io.vertx.core.Vertx
import io.vertx.core.logging.LoggerFactory
import io.vertx.kotlin.config.ConfigRetrieverOptions
import io.vertx.kotlin.config.ConfigStoreOptions
import io.vertx.kotlin.core.json.array
import io.vertx.kotlin.core.json.json
import io.vertx.kotlin.core.json.obj





/**
 * configuration manager for all configs
 * @author Razak Zakari<razak@transcodium.com>
 */
class ConfigManager {

    companion object {

        val logger by lazy{
            LoggerFactory.getLogger(this::class.java)
        }


        /**
         * configRetriver
         */
        private var configRetriever: ConfigRetriever? = null

        private val configDir by lazy {

            val jarPath = basePath()

            val configPath = jarPath.resolve("config")

            return@lazy configPath
        }//end


        /**
         * fetch - retrieve current config
         * @return json
         * @param
         */
        fun retrive(vertx: Vertx? = null): ConfigRetriever {

            /**
             * if config exists already, then return it
             */
            if (configRetriever != null) {
                return configRetriever!!
            }

            //set hazlecast and vertx other config
            System.setProperty("config.dir",configDir.absolutePath)


            //config files to load
            val configfiles = mutableListOf(
                    "app.conf",
                    "database.conf",
                    "drivers.conf"
            )

            //check if any of the config files are missing
            configfiles.forEach { configFileName ->

                val fileStorePath = configDir.resolve(configFileName)

                /**
                 * if the config Dir is empty, set error
                 */
                if (!fileStorePath.isFile) {
                    logger.fatal("The config file '$configFileName' cannot be found")
                    System.exit(1)
                }//end if

            }//end loop


            /**
             * load configuration from directory
             */
            val dirStore  = ConfigStoreOptions(
                    type = "directory",
                    config = json {
                        obj(
                                "path" to "config",
                                "filesets" to array(
                                        obj(
                                                "pattern" to "*.conf",
                                                "format" to "hocon"
                                        ))
                        )
                    }
            )//end dirstore config scanner



            //load up all the config stores
            var options = ConfigRetrieverOptions(
                    stores = listOf(dirStore),
                    scanPeriod = 2000
            )

            //get retriever
            configRetriever = ConfigRetriever.create(
                    vertx ?: Vertx.vertx(),options
            )

            return configRetriever!!
        }//end fun


    }//enc companion obj


}