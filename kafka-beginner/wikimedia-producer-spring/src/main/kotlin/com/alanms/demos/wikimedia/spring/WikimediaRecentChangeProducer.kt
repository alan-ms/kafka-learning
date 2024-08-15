package com.alanms.demos.wikimedia.spring

import com.launchdarkly.eventsource.EventSource
import com.launchdarkly.eventsource.background.BackgroundEventSource
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.boot.ApplicationRunner
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import java.net.URI
import java.util.concurrent.TimeUnit

@Component
class WikimediaRecentChangeProducer {

    @Bean
    fun wikimediaRecentChangesTopic() = NewTopic(TOPIC_NAME, 1,1)

    @Bean
    fun runner(template: KafkaTemplate<String, String>) =
        ApplicationRunner {
            val url = "https://stream.wikimedia.org/v2/stream/recentchange"

            val backgroundEventHandler = WikimediaChangeHandler(template)
            val builder = EventSource.Builder(URI.create(url))

            val backgroundEventSource = BackgroundEventSource.Builder(
                backgroundEventHandler,
                builder
            ).build()

            backgroundEventSource.use {
                backgroundEventSource.start()
                TimeUnit.MINUTES.sleep(10)
            }
        }
}
