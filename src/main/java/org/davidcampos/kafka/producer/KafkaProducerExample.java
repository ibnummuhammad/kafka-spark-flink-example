package org.davidcampos.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.davidcampos.kafka.commons.Commons;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class KafkaProducerExample {
    private static final Logger logger = LogManager
            .getLogger(KafkaProducerExample.class);

    public static void main(final String... args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Commons.EXAMPLE_KAFKA_SERVER);
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducerExample");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        String[] words = new String[] { "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten" };
        Random ran = new Random(System.currentTimeMillis());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        logger.info("Add producer close");

        try {
            while (true) {
                String word = words[ran.nextInt(words.length)];
                String uuid = UUID.randomUUID().toString();

                ProducerRecord<String, String> record = new ProducerRecord<>(Commons.EXAMPLE_KAFKA_TOPIC,
                        uuid, word);
                producer.send(record);
                logger.info("Sent ({}, {}) to topic {}.", uuid, word, Commons.EXAMPLE_KAFKA_TOPIC);

                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
