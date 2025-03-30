package com.alanms.demos.bankbalance.streams

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration

@Configuration
@EnableKafka
@EnableKafkaStreams
class BankBalanceStreamConfig {

    private val TRANSACTION_INPUT_TOPIC = "bankbalance.transaction"
    private val ACCOUNT_BALANCE_TOPIC = "bankbalance.account.balance"
    private val APPLICATION_ID = "bankbalance-account-balance-stream"
    private val BOOTSTRAP_SERVER = "localhost:9092"
    private val AUTO_OFFSET_RESET_CONFIG = "earliest"
    private val OBJECT_MAPPER = ObjectMapper()

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfigs(): KafkaStreamsConfiguration {
        val props = mutableMapOf<String, Any>()

        props[StreamsConfig.APPLICATION_ID_CONFIG] = APPLICATION_ID
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVER
        props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = AUTO_OFFSET_RESET_CONFIG

        // Exactly once processing!!
        props[StreamsConfig.PROCESSING_GUARANTEE_CONFIG] = StreamsConfig.EXACTLY_ONCE_V2

        return KafkaStreamsConfiguration(props)
    }

    @Bean
    fun buildBankBalance(kStreamsBuilder: StreamsBuilder): Topology {
        val transactionInput = kStreamsBuilder.stream<String, String>(TRANSACTION_INPUT_TOPIC)

        val accountAndBalance = transactionInput
            .mapValues { _, transaction -> OBJECT_MAPPER.readTree(transaction)[BankBalanceTransactionAttributes.AMOUNT.attrName].asDouble() }
            .groupByKey()
            .aggregate(
                { 0.0 },
                { _, newAmount, aggregate -> aggregate + newAmount },
                Materialized.with(Serdes.String(), Serdes.Double())
            )

        accountAndBalance.toStream().to(ACCOUNT_BALANCE_TOPIC, Produced.with(Serdes.String(), Serdes.Double()))

        return kStreamsBuilder.build()
    }
}
