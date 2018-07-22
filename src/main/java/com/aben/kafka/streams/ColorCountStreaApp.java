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
 * Created by hadoop on 7/17/18.
 */
public class ColorCountStreaApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "color-count-app-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
        // 1 - stream from Kafka

        KStream<String, String> streamColors = builder.stream("color-count-input");
        KStream<String, String> colorCounts = streamColors
                .filter((name, color) -> !name.isEmpty())
                .selectKey((name, color) -> color) // (color, color)
                .mapValues(inputStream -> inputStream.toLowerCase())
                .filter((name, color) -> color.equals("red") || color.equals("blue") || color.equals("green"));

        colorCounts.to("colorCounts");

        KTable<String, String> usersAndColorsTable = builder.table("colorCounts");

        KTable<String, Long> favoriteColors = usersAndColorsTable
                .groupBy((name, color) -> new KeyValue<>(color, color))
                .count("CountsColor");
        favoriteColors.to(Serdes.String(), Serdes.Long(), "color-count-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        //print the toplogy
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
