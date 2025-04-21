package com.alanms.demos.bankbalance.producer

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate

@Configuration
class KafkaProducerConfig {

    private fun producerConfigs(): MutableMap<String, Any> {
        val props = hashMapOf<String, Any>()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        props[ProducerConfig.COMPRESSION_TYPE_CONFIG] = "snappy"
        props[ProducerConfig.ACKS_CONFIG] = "all"
        props[ProducerConfig.LINGER_MS_CONFIG] = 1
        props[ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG] = "true"
        props[ProducerConfig.RETRIES_CONFIG] = 3

        return props
    }

    @Bean
    fun producerFactory(): DefaultKafkaProducerFactory<String, String> {
        return DefaultKafkaProducerFactory<String, String>(producerConfigs())
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(producerFactory())
    }
}
