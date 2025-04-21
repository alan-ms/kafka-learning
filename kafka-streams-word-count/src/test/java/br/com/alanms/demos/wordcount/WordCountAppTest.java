package br.com.alanms.demos.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.Properties;

import static br.com.alanms.demos.wordcount.WordCountApp.TOPIC_INPUT;
import static br.com.alanms.demos.wordcount.WordCountApp.TOPIC_OUTPUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class WordCountAppTest {

    public TopologyTestDriver testDriver;

    public TestInputTopic<String, String> testInputTopic;
    public TestOutputTopic<String, Long> testOutputTopic;

    @BeforeEach
    public void setupTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology wordCountTopology = WordCountApp.wordCountTopologyBuilder();

        testDriver = new TopologyTestDriver(wordCountTopology, config);

        testInputTopic = testDriver.createInputTopic(TOPIC_INPUT, new StringSerializer(), new StringSerializer());
        testOutputTopic = testDriver.createOutputTopic(TOPIC_OUTPUT, new StringDeserializer(), new LongDeserializer());
    }

    @AfterEach
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void makeSureCountAreCorrect() {
        String firstExample = "testing Kafka Streams StReAMs kafka";
        pushNewInputRecord(firstExample);

        assertEquals(readOutput(), new KeyValue<>("testing", 1L));
        assertEquals(readOutput(), new KeyValue<>("kafka", 1L));
        assertEquals(readOutput(), new KeyValue<>("streams", 1L));
        assertEquals(readOutput(), new KeyValue<>("streams", 2L));
        assertEquals(readOutput(), new KeyValue<>("kafka", 2L));
        assertThrows(NoSuchElementException.class, this::readOutput);
    }

    public void pushNewInputRecord(String value) {
        testInputTopic.pipeInput("word-count-input", value);
    }

    public KeyValue<String, Long> readOutput() {
        return testOutputTopic.readKeyValue();
    }
}
