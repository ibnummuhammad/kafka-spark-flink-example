package org.davidcampos.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.davidcampos.kafka.commons.Commons;

import java.util.Collections;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KafkaConsumerExample {
    private static final Logger logger = LogManager
            .getLogger(KafkaConsumerExample.class);

    public static void main(final String... args) {
        ConcurrentMap<String, Integer> counters = new ConcurrentHashMap<>();

        // Create properties
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Commons.EXAMPLE_KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerGroup");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        // Create the consumer_1 using properties
        final Consumer<String, String> consumer_1 = new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer_1.subscribe(Collections.singletonList(Commons.EXAMPLE_KAFKA_TOPIC));

        final Consumer<String, String> consumer = consumer_1;

        logger.info("Add Properties setProperty()");

        while (true) {
            final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMinutes(1));
            consumerRecords.forEach(record -> {
                String word = record.value();

                int count = counters.containsKey(word) ? counters.get(word) : 0;
                counters.put(word, ++count);

                logger.info("({}, {})", word, count);
            });
            consumer.commitAsync();
        }
    }
}
