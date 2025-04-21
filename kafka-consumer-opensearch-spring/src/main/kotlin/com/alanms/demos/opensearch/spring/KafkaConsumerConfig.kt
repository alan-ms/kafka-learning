package com.alanms.demos.opensearch.spring

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties

@EnableKafka
@Configuration
class KafkaConsumerConfig(private val defaultKafkaConsumerFactory: DefaultKafkaConsumerFactory<String, String>) {

    @Bean
    fun wikimediaRecentChangeListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = defaultKafkaConsumerFactory
        factory.setConcurrency(1)
        factory.isBatchListener = true
        factory.containerProperties.pollTimeout = 1000
        factory.containerProperties.ackMode = ContainerProperties.AckMode.MANUAL
        factory.containerProperties.isSyncCommits = true
        return factory
    }
}
