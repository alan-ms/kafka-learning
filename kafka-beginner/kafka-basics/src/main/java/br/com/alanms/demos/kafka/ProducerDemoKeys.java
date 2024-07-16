package br.com.alanms.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        // create Producer Properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "");
        properties.put("sasl.mechanism", "SCRAM-SHA-256");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"bmV3LXJlaW5kZWVyLTExMDEzJN5Y8eZGikTWTqlES1zw8QrI_WJNaweG4Ty_Gaw\" password=\"Mjk1MTVhMzgtZDg4NS00MTA0LWIwNmMtMDkyOTA0MGY4Nzkx\";");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 3; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world kafka: " + i;

                // create a Producer Record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, (metadata, e) -> { // callback
                    // executes every time a record successfully send or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Received new metadata \n" +
                                "Topic: {} \n" +
                                "Key: {} Partition: {} \n" +
                                "Offset: {} \n" +
                                "Timestamp: {} \n", metadata.topic(), key, metadata.partition(), metadata.offset(), metadata.timestamp()
                        );
                    } else {
                        log.error("Erro while producer", e);
                    }
                });
            }
        }

        // tell the producer to send all data and block until done - synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
