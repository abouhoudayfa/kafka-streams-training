package com.aben.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by abouhoudayfa on 7/17/18.
 cd /usr/local/kafka
 bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic color-input-color
 bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic color-output-color
 bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic user-key-color
 //list topics
 bin/kafka-topics.sh --list --zookeeper localhost:2181
 //create consumer
 bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic word-count-output --from-beginning --property print.key=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 //create producer
 cd /usr/local/kafka
 bin/kafka-console-producer.sh --broker-list localhost:9092 --topic word-count-input   --property "parse.key=true" \
 --property "key.separator=,"
 */
public class ColorCountStreaApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "color-count-app-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> streamColors = builder.stream("color-count-input");
        streamColors
                .filter((name, color) -> !name.isEmpty())
                .selectKey((name, color) -> color)
                .mapValues(color -> color.toLowerCase())
                .filter((name, color) -> color.equals("red") || color.equals("blue") || color.equals("green"))//Arrays.asList(
                .to("user-key-color");

        KTable<String, String> usersAndColorsTable = builder.table("user-key-color");

        KTable<String, Long> favoriteColors = usersAndColorsTable
                .groupBy((name, color) -> new KeyValue<>(color, color))
                .count("CountsColor");
        favoriteColors.to(Serdes.String(), Serdes.Long(), "color-count-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.cleanUp();
        streams.start();

        //print the toplogy
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
