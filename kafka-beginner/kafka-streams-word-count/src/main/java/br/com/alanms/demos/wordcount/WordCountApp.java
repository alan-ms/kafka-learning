package br.com.alanms.demos.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    private static final String APPLICATION_NAME = "streams-word-count";
    private static final String KAFKA_CLUSTER_URL = "localhost:9092";
    private static final String AUTO_OFFSET_RESET_CONFIG = "earliest";
    public static final String TOPIC_INPUT = "word-count-input";
    public static final String TOPIC_OUTPUT = "word-count-output";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_URL);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_CONFIG);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology wordCountTopology = wordCountTopologyBuilder();

        KafkaStreams streams = new KafkaStreams(wordCountTopology, config);
        streams.start();
        // printed the topology
        System.out.println(streams);
        // graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Topology wordCountTopologyBuilder() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> wordCountInput = builder.stream(TOPIC_INPUT);

        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())
                .flatMapValues(lowercasedTextLine -> Arrays.asList(lowercasedTextLine.split(" ")))
                .selectKey((ignoredKey, word) -> word)
                .groupByKey()
                .count();

        wordCounts.toStream().to(TOPIC_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }
}
