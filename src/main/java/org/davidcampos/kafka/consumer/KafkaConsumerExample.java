package org.davidcampos.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.davidcampos.kafka.commons.Commons;

import java.util.Arrays;
import java.time.Duration;
import java.util.Properties;

public class KafkaConsumerExample {
    private static final Logger logger = LogManager
            .getLogger(KafkaConsumerExample.class);

    public static void main(final String... args) {
        // Create properties
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Commons.EXAMPLE_KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerGroup");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using properties
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Arrays.asList(Commons.EXAMPLE_KAFKA_TOPIC));

        logger.info("Remove unused modules");

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(
                        Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("Key: " + record.key() + ", Value: " + record.value()
                            + ", Partition: " + record.partition() + ", Offset:"
                            + record.offset());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
