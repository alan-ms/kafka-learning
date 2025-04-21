package com.alanms.demos.opensearch.spring

import com.google.gson.JsonParser
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.opensearch.client.RequestOptions
import org.opensearch.client.RestClient
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.io.IOException
import java.net.URI

@Service
class OpenSearchService {

    private val log: Logger = LoggerFactory.getLogger(OpenSearchService::class.simpleName)

    @Value("\${opensearch.index.name:wikimedia}")
    private lateinit var INDEX_NAME_OPEN_SEARCH: String

    fun createOpenSearchClient(): RestHighLevelClient {
        val connString = ""

        // we build a URI from connection string
        val restHighLevelClient: RestHighLevelClient
        val connURI = URI.create(connString)
        // extract login information if it exists
        val userInfo = connURI.userInfo

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = RestHighLevelClient(RestClient.builder(HttpHost(connURI.host, connURI.port, "http")))
        } else {
            // REST client with security
            val auth = userInfo
                .split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()

            val cp: CredentialsProvider = BasicCredentialsProvider()
            cp.setCredentials(AuthScope.ANY, UsernamePasswordCredentials(auth[0], auth[1]))
            restHighLevelClient = RestHighLevelClient(
                RestClient.builder(HttpHost(connURI.host, connURI.port, connURI.scheme)).setHttpClientConfigCallback { httpAsyncClientBuilder: HttpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(
                        cp
                    ).setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy())
                }
            )
        }
        return restHighLevelClient
    }

    fun extractId(json: String?): String {
        return JsonParser.parseString(json).asJsonObject["meta"]
            .asJsonObject["id"]
            .asString
    }

    @Throws(IOException::class)
    fun createIndexOpenSearch(openSearchClient: RestHighLevelClient) {
        // we need to create index on OpenSearch if it doesn't exist already
        val indexExists = openSearchClient.indices()
            .exists(GetIndexRequest(INDEX_NAME_OPEN_SEARCH), RequestOptions.DEFAULT)
        log.info("index: {}", indexExists)
        if (!indexExists) {
            val createIndexRequest = CreateIndexRequest(INDEX_NAME_OPEN_SEARCH)
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT)
            log.info("The Wikimedia Index has been created!")
        } else {
            log.info("The Wikimedia Index already exists")
        }
    }
}
