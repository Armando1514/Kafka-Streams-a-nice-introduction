package com.github.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class UserEnrichmentApp {

  public Topology createTopology() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    GlobalKTable<String,String> users = streamsBuilder.globalTable("user-table");
    KStream<String, String> purchases = streamsBuilder.stream("purchases");

    // Join
    KStream<String, String> userPurchasesJoin = purchases.join(
      users,
      (key, value) -> key,
      (purchase, user) -> "Purchase=" + purchase + ",UserInfo=[" + user + "]"
    );
    userPurchasesJoin.to("user-purchases-join");

    // Left Join
    KStream<String, String> userPurchaseLeftJoin = purchases.leftJoin(
      users,
      (key, value) -> key,
      (purchase, user) -> {
        if (user != null) {
          return "Purchase=" + purchase + ",UserInfo=[" + user + "]";
        } else {
          return "Purchase=" + purchase + ",UserInfo=null";
        }
      }
    );
    userPurchaseLeftJoin.to("user-purchases-left-join");

    return streamsBuilder.build();
  }

  public static void main(String[] args) {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-enrichment-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    UserEnrichmentApp userEnrichmentApp = new UserEnrichmentApp();

    // Create the streams
    Topology topology = userEnrichmentApp.createTopology();
    KafkaStreams streams = new KafkaStreams(topology, config);
    streams.start();

    // Print the topology
    System.out.println(streams.toString());

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

  }

}
