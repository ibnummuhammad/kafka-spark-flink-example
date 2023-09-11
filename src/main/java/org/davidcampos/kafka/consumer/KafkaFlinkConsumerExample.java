package org.davidcampos.kafka.consumer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.davidcampos.kafka.commons.Commons;

import java.util.Properties;

public class KafkaFlinkConsumerExample {
    private static final Logger logger = LogManager
            .getLogger(KafkaFlinkConsumerExample.class);

    public static void main(final String... args) {

        System.out.println("Add dataStream");

        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Row> dataStream = env.fromElements(Row.of("Alice", 12),
                Row.of("Bob", 10), Row.of("Alice", 100), Row.of("Lucy", 50));

        System.out.println("ini dataStream");
        System.out.println(dataStream);

        // // Properties
        // final Properties props = new Properties();
        // props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        // Commons.EXAMPLE_KAFKA_SERVER);
        // props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "FlinkConsumerGroup");

        // FlinkKafkaConsumer010<String> flinkSource = new FlinkKafkaConsumer010<>(
        // Commons.EXAMPLE_KAFKA_TOPIC, new SimpleStringSchema(), props);

        // DataStream<String> messageStream = env.addSource(flinkSource);

        // // Split up the lines in pairs (2-tuples) containing: (word,1)
        // messageStream.flatMap(new Tokenizer())
        // // group by the tuple field "0" and sum up tuple field "1"
        // .keyBy(0).sum(1).print();

        // try {
        // env.execute("flink-read");
        // } catch (Exception e) {
        // logger.error("An error occurred.", e);
        // }
    }

    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
