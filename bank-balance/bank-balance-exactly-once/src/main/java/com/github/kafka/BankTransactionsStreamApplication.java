package com.github.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;
public class BankTransactionsStreamApplication {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-colour-count");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        //json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> transactionsInput = builder.stream("bank-transactions-input",
                Consumed.with(Serdes.String(), jsonSerde));


        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("balance", 0);
        initialBalance.put("lastUpdated", Instant.ofEpochMilli(0L).toString());


        KTable<String, JsonNode> accountBalances = transactionsInput
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> initialBalance,
                        (aggregateKey, newTransaction, currentTransaction) -> updateBalance(newTransaction, currentTransaction),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("account-balance-aggregator")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(jsonSerde)
                );

        accountBalances.toStream().to("bank-transactions-output", Produced.with(Serdes.String(), jsonSerde));
        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.start();
        // print the topology
        System.out.println(streams.toString());

        // Add shutdown hook to stop the Kafka Streams threads.
        // You can optionally provide a timeout to 'close'
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static JsonNode updateBalance(JsonNode newTransaction, JsonNode currentTransaction) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("balance", currentTransaction.get("balance").asInt() + newTransaction.get("amount").asInt());

        long balanceEpoch = Instant.parse(currentTransaction.get("lastUpdated").asText()).toEpochMilli();
        long transactionEpoch = Instant.parse(newTransaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("lastUpdated", newBalanceInstant.toString());
        return newBalance;
    }
}
