package org.davidcampos.kafka.consumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.davidcampos.kafka.commons.Commons;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KafkaSparkConsumerExample {
    private static final Logger logger = LogManager
            .getLogger(KafkaSparkConsumerExample.class);

    public static void main(final String... args) {
        String cetak = "Add 'select word, count(*) as total from words group by word'";
        logger.info(cetak);
        System.out.println(cetak);

        // Configure Spark to connect to Kafka running on local machine
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                Commons.EXAMPLE_KAFKA_SERVER);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "SparkConsumerGroup");

        // Configure Spark to listen messages in topic test
        Collection<String> topics = Arrays.asList(Commons.EXAMPLE_KAFKA_TOPIC);

        SparkConf conf = new SparkConf().setMaster("local[2]")
                .setAppName("SparkConsumerApplication");

        // Read messages in batch of 30 seconds
        JavaStreamingContext jssc = new JavaStreamingContext(conf,
                Durations.seconds(5));

        // Start reading messages from Kafka and get DStream
        final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
                .createDirectStream(jssc, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));

        // Read value of each message from Kafka and return it
        JavaDStream<String> lines = stream.map(
                (Function<ConsumerRecord<String, String>, String>) kafkaRecord -> kafkaRecord
                        .value());

        // lines.print();

        // Break every message into words and return list of words
        JavaDStream<String> words = lines
                .flatMap((FlatMapFunction<String, String>) line -> Arrays
                        .asList(line.split(" ")).iterator());

        // Print the word count
        words.print();

        // // Take every word and return Tuple with (word,1)
        // JavaPairDStream<String, Integer> wordMap = words.mapToPair(
        // (PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        // // // Print the word count
        // // wordMap.print();

        // // Count occurrence of each word
        // JavaPairDStream<String, Integer> wordCount = wordMap
        // .reduceByKey((Function2<Integer, Integer, Integer>) (first,
        // second) -> first + second);

        // // // Print the word count
        // // wordCount.print();

        words.foreachRDD((rdd, time) -> {
            SparkSession spark = JavaSparkSessionSingleton
                    .getInstance(rdd.context().getConf());

            // Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
            JavaRDD<JavaRecord> rowRDD = rdd.map(word -> {
                JavaRecord record = new JavaRecord();
                record.setWord(word);
                return record;
            });
            Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD,
                    JavaRecord.class);

            // Creates a temporary view using the DataFrame
            wordsDataFrame.createOrReplaceTempView("words");

            // Do word count on table using SQL and print it
            Dataset<Row> wordCountsDataFrame = spark
                    .sql("select word, count(*) as total from words group by word");
            System.out.println("========= " + time + "=========");
            wordCountsDataFrame.show();

            System.out.println("ini wordsDataFrame");
            System.out.println(wordsDataFrame);
        });

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            logger.error("An error occurred.", e);
        }
    }
}

class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;

    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession.builder().config(sparkConf).getOrCreate();
        }
        return instance;
    }
}
