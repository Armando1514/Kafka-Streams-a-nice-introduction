package com.github.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        // 2. MapValues lowercase ( we want all the words to be in lowercase).
        KTable<String, Long> wordCounts = wordCountInput.mapValues((value) -> value.toLowerCase())

                // 3. FlatMapValues split by space.
                .flatMapValues(value -> Arrays.asList(value.split(" ")))

                // 4. SelectKey to apply a key (replace the key of the message, and we want the key to be equal to the value).
                .selectKey((ignoredKey, value) -> value)

                // 5. GroupByKey before aggregation (because before we defined the key as value, there are going to be sets with all the same word).
                .groupByKey()

                // 6. Count occurences in each group.
                .count();

        // 7. To in order to write the result back to kafka.
        // We need to specify that the key are going to be string (Serdes.String)
        // and that values are going to be Long (Serdes.Long)
        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        // At this point the builder creates the application from the builder with the config defined before.
        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // start the application
        streams.start();

        // print the topology
        System.out.println(streams.toString());

        // Add shutdown hook to stop the Kafka Streams threads.
        // You can optionally provide a timeout to 'close'
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
