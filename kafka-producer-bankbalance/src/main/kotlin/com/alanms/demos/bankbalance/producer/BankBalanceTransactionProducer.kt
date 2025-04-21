package com.alanms.demos.bankbalance.producer

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.boot.ApplicationRunner
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.LocalDateTime
import kotlin.random.Random

@Component
class BankBalanceTransactionProducer {

    private val TOPIC_NAME = "bankbalance.transaction"
    private val OBJECT_MAPPER = ObjectMapper()
    private val CUSTOMER_NAMES = mutableListOf("Four Kings", "Nito", "Gwyn", "Gapping Dragon", )
    private val MAP_CUSTOMER_BALANCE = mutableMapOf<String, BigDecimal>()

    @Bean
    fun runner(kafkaTemplate: KafkaTemplate<String, String>) = ApplicationRunner {
        while (true) {
            val transaction = BankBalanceTransactionDTO(CUSTOMER_NAMES.random(), generateRandomAmount(), LocalDateTime.now().toString())
            val producerRecord = ProducerRecord(TOPIC_NAME, transaction.name.uppercase(), OBJECT_MAPPER.writeValueAsString(transaction))
            kafkaTemplate.send(producerRecord)
            sumCredits(transaction) // Para verificar o resultado com a stream Kafka
            Thread.sleep(2000)
        }
    }

    fun generateRandomAmount(): BigDecimal = BigDecimal.valueOf(1 + (99 * Random.nextDouble()))

    fun sumCredits(transaction: BankBalanceTransactionDTO) {
        MAP_CUSTOMER_BALANCE.compute(transaction.name) { _, balance ->
            (balance ?: BigDecimal.ZERO).add(transaction.amount)
        }
        println("CUSTOMER ${transaction.name} | ADD ${transaction.amount} | TOTAL ${MAP_CUSTOMER_BALANCE[transaction.name]}")
    }
}
