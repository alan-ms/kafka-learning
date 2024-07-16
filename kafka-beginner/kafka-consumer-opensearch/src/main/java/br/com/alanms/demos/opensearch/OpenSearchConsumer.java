package br.com.alanms.demos.opensearch;

import com.google.gson.Gson;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;

import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    private static final String INDEX_NAME_OPEN_SEARCH = "wikimedia";

    private static final String TOPIC_NAME = "wikimedia.recentchange";

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "";

        // we build a URI from connection string
        RestHighLevelClient restHighLevelClient;
        URI connURI = URI.create(connString);
        // extract login information if it exists
        String userInfo = connURI.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connURI.getHost(), connURI.getPort(), "http")));
        } else {
            // REST client with security
            String[] auth = userInfo
                    .split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connURI.getHost(), connURI.getPort(), connURI.getScheme())).setHttpClientConfigCallback(
                            httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp).setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                    )
            );
        }
        return restHighLevelClient;
    }

    public static void createIndexOpenSearch(RestHighLevelClient openSearchClient) throws IOException {
        // we need to create index on OpenSearch if it doesn't exist already
        boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest(INDEX_NAME_OPEN_SEARCH), RequestOptions.DEFAULT);
        if (!indexExists) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME_OPEN_SEARCH);
            openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            log.info("The Wikimedia Index has been created!");
        } else {
            log.info("The Wikimedia Index already exists");
        }
    }

    public static KafkaConsumer<String, String> createKafkaConsumer() {
        String boostrapServers = "127.0.0.1:9092";
        String groupId = "consumer-opensearch-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // to asynchronous operations

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();
            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        return consumer;
    }

    public static String extractId(String json) {
        return JsonParser.parseString(json).getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        // first create on OpenSearch Client and index
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        createIndexOpenSearch(openSearchClient);
        // create our Kafka Client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        consumer.subscribe(Collections.singleton(TOPIC_NAME));
        // main code logic

        BulkRequest bulkRequest = new BulkRequest();

        try (openSearchClient; consumer) {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                log.info("Received: {} record(s)", records.count());

                for (ConsumerRecord<String, String> r: records) {
                    // Impedance Consumer strategy 1 - define an ID using Kafka Record coordinates
//                    String id = r.topic() + "_" + r.partition() + "_" + r.offset();
                    // Impedance Consumer strategy 2 - we extract ID from JSON value
                    String id = extractId(r.value());
                    // send the record into OpenSearch
                    IndexRequest indexRequest = new IndexRequest(INDEX_NAME_OPEN_SEARCH)
                            .source(r.value(), XContentType.JSON)
                            .id(id);
//                    IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                    log.info(response.getId());
                    bulkRequest.add(indexRequest);
                }

                // optimized with seeded bulk request
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted {} record(s).", bulkResponse.getItems().length);

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    // commit offsets after the batch is consumed
                    consumer.commitSync();
                    log.info("Offsets have been commited!");
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            log.info("The consumer is now gracefully shut down");
        }
    }
}
