package com.alanms.demos.wikimedia.spring

import com.launchdarkly.eventsource.MessageEvent
import com.launchdarkly.eventsource.background.BackgroundEventHandler
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate

class WikimediaChangeHandler(private val kafkaTemplate: KafkaTemplate<String, String>): BackgroundEventHandler {

    private val log = LoggerFactory.getLogger(WikimediaChangeHandler::class.simpleName)

    override fun onOpen() {
        // nothing here
    }

    override fun onClosed() {
        // nothing here
    }

    override fun onMessage(s: String, messageEvent: MessageEvent) {
        log.info(messageEvent.data)
        val producerRecord = ProducerRecord<String, String>(TOPIC_NAME, messageEvent.data)
        // asynchronous
        kafkaTemplate.send(producerRecord)
    }

    override fun onComment(c: String?) {
        // nothing here
    }

    override fun onError(t: Throwable?) {
        log.error("Error in Stream Reading", t)
    }
}
