package com.alanms.demos.opensearch.spring

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.RestHighLevelClient
import org.opensearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.Acknowledgment
import org.springframework.stereotype.Component

@Component
class WikimediaRecentChangeConsumer(private val openSearchService: OpenSearchService) {

    private val log = LoggerFactory.getLogger(WikimediaRecentChangeConsumer::class.simpleName)

    private final var openSearchClient: RestHighLevelClient = openSearchService.createOpenSearchClient()
    private final var bulkRequest: BulkRequest

    @Value("\${opensearch.index.name:wikimedia}")
    private lateinit var INDEX_NAME_OPEN_SEARCH: String

    init {
        openSearchService.createIndexOpenSearch(openSearchClient)
        bulkRequest = BulkRequest()
    }

    @KafkaListener(topics = ["wikimedia.recentchange"], groupId = "consumer-opensearch-demo", containerFactory = "wikimediaRecentChangeListenerContainerFactory")
    fun consume(records: List<ConsumerRecord<String, String>>, ack: Acknowledgment) {
        log.info("Received: {} record(s)", records.size)
        records.forEach {
            val id = openSearchService.extractId(it.value())
            val indexRequest = IndexRequest(INDEX_NAME_OPEN_SEARCH).source(it.value(), XContentType.JSON)
                .id(id)
            this.bulkRequest.add(indexRequest)
        }

        if (this.bulkRequest.numberOfActions() > 0) {
            val bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT)
            log.info("Inserted {} record(s).", bulkResponse.items.size)

            try {
                Thread.sleep(2000)
            } catch (e : InterruptedException) {
                e.printStackTrace()
            }

            // commit offsets after the batch is consumed
            ack.acknowledge()
            log.info("Offsets have been commited!")

        }
    }
}
