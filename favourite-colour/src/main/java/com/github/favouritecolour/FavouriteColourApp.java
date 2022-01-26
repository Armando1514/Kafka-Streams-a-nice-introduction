package com.github.favouritecolour;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class FavouriteColourApp {

    public enum Colours {
        RED,
        BLUE,
        GREEN;

        public static boolean isAValidColour(String InputColour){
            return Stream.of(Colours.values()).anyMatch(e -> e.toString().equals(InputColour));
        }

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-count");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> favouriteColoursInput = builder.stream("favourite-colour-input-topic" /* input topic */);
        KStream<String, String> favouriteColours = favouriteColoursInput.map((key, value) -> new KeyValue<>(
                value.split(",")[0].toLowerCase(),
                value.split(",")[1].toLowerCase())
                )
                .filter((ignoredKey, value) -> Colours.isAValidColour(value));

        favouriteColours.to("mid-favourite-colour-output-topic", Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> midFavouriteColourInput = builder.table("mid-favourite-colour-output-topic" /* input topic */);
        KTable<String, Long> countFavouriteColours = midFavouriteColourInput.groupBy((user, colour) -> new KeyValue<>(colour, colour)).count();

        countFavouriteColours.toStream().to("favourite-colour-output-topic", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.start();
        // print the topology
        System.out.println(streams.toString());

        // Add shutdown hook to stop the Kafka Streams threads.
        // You can optionally provide a timeout to 'close'
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
