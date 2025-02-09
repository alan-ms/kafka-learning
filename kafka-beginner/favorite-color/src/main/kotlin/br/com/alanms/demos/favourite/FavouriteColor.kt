package br.com.alanms.demos.favourite

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import java.util.*

class FavouriteColor {

    companion object {
        val APPLICATION_NAME = "streams-favourite-color"
        val KAFKA_CLUSTER_URL = "localhost:9092"
        val AUTO_OFFSET_RESET_CONFIG = "earliest"
        val TOPIC_INPUT = "favourite-color-input"
        val TOPIC_OUTPUT = "favourite-color-output"
        val TOPIC_USE_KEY_FAVOURITE_COLOR = "user-key-favourite-color"
        val TOPIC_FAVOURITE_COLOR_COUNT = "favourite-colors-count"
        val LIST_COLORS_PERMITTED = listOf("green", "red", "blue")
        val COMMA = ","


        @JvmStatic
        fun main(args: Array<String>) {
            val config = Properties()
            config[StreamsConfig.APPLICATION_ID_CONFIG] = APPLICATION_NAME
            config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = KAFKA_CLUSTER_URL
            config[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = AUTO_OFFSET_RESET_CONFIG
            config[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass
            config[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass

            val builder = StreamsBuilder()

            val favouriteColorInput = builder.stream<String, String>(TOPIC_INPUT)

            favouriteColorInput
                .mapValues { value -> value.lowercase() }
                .selectKey { _, value -> value.split(COMMA)[0] }
                .mapValues{ value -> value.split(COMMA)[1] }
                .filter { _, color -> LIST_COLORS_PERMITTED.contains(color) }
                .to(TOPIC_USE_KEY_FAVOURITE_COLOR)


            val userAndFavouriteColorTable = builder.table<String, String>(TOPIC_USE_KEY_FAVOURITE_COLOR)


            val favouriteColorsCount = userAndFavouriteColorTable
                .groupBy { _, color -> KeyValue(color, color) }
                .count(Named.`as`(TOPIC_FAVOURITE_COLOR_COUNT))

            favouriteColorsCount.toStream().to(TOPIC_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()))

            val streams = KafkaStreams(builder.build(), config)
            streams.cleanUp() // for development
            streams.start()
            // printed the topology
            println(streams)
            // graceful shutdown
            Runtime.getRuntime().addShutdownHook(Thread { streams.close() })
        }
    }
}

