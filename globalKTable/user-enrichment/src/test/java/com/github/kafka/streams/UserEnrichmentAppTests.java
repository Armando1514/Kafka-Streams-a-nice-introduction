package com.github.kafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class UserEnrichmentAppTests {

  private TopologyTestDriver testDriver;
  private StringSerializer stringSerializer = new StringSerializer();
  private ConsumerRecordFactory<String, String> recordFactory =
    new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

  @Before
  public void setupTopologyTestDriver() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-enrichment-application");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "testdriver:1234");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    UserEnrichmentApp userEnrichmentApp = new UserEnrichmentApp();
    Topology topology = userEnrichmentApp.createTopology();

    testDriver = new TopologyTestDriver(topology, config);
  }

  @After
  public void closeTestDriver() {
    testDriver.close();
  }

  private void streamUserRecord(String key, String value) {
    testDriver.pipeInput((recordFactory.create("user-table", key, value)));
  }

  private void streamPurchaseRecord(String key, String value) {
    testDriver.pipeInput((recordFactory.create("purchases", key, value)));
  }

  private ProducerRecord<String, String> readJoinOutputRecord() {
    return testDriver.readOutput("user-purchases-join", new StringDeserializer(), new StringDeserializer());
  }

  private ProducerRecord<String, String> readLeftJoinOutputRecord() {
    return testDriver.readOutput("user-purchases-left-join", new StringDeserializer(), new StringDeserializer());
  }

  @Test
  public void userPurchasesTest() {
    System.out.println("\nExample 1 - new user\n");
    streamUserRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com");
    streamPurchaseRecord("john", "Apples and Bananas (1)");
    assertEquals("Purchase=Apples and Bananas (1),UserInfo=[First=John,Last=Doe,Email=john.doe@gmail.com]", readJoinOutputRecord().value());
    assertEquals("Purchase=Apples and Bananas (1),UserInfo=[First=John,Last=Doe,Email=john.doe@gmail.com]", readLeftJoinOutputRecord().value());

    // 2 - we receive user purchase, but it doesn't exist in Kafka
    System.out.println("\nExample 2 - non existing user\n");
    streamPurchaseRecord("bob", "Kafka Udemy Course (2)");
    assertNull(readJoinOutputRecord());
    assertEquals("Purchase=Kafka Udemy Course (2),UserInfo=null", readLeftJoinOutputRecord().value());

    // 3 - we update user "john", and send a new transaction
    System.out.println("\nExample 3 - update to user\n");
    streamUserRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com");
    streamPurchaseRecord("john", "Oranges (3)");
    assertEquals("Purchase=Oranges (3),UserInfo=[First=Johnny,Last=Doe,Email=johnny.doe@gmail.com]", readJoinOutputRecord().value());
    assertEquals("Purchase=Oranges (3),UserInfo=[First=Johnny,Last=Doe,Email=johnny.doe@gmail.com]", readLeftJoinOutputRecord().value());

    // 4 - we send a user purchase for stephane, but it exists in Kafka later
    System.out.println("\nExample 4 - non existing user then user\n");
    streamPurchaseRecord("stephane", "Computer (4)");
    assertNull(readJoinOutputRecord());
    assertEquals("Purchase=Computer (4),UserInfo=null", readLeftJoinOutputRecord().value());

    streamUserRecord("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph");
    streamPurchaseRecord("stephane", "Books (4)");
    assertEquals("Purchase=Books (4),UserInfo=[First=Stephane,Last=Maarek,GitHub=simplesteph]", readJoinOutputRecord().value());
    assertEquals("Purchase=Books (4),UserInfo=[First=Stephane,Last=Maarek,GitHub=simplesteph]", readLeftJoinOutputRecord().value());

    streamUserRecord("stephane", null);
    assertNull(readJoinOutputRecord());
    assertNull(readLeftJoinOutputRecord());

    // 5 - we create a user, but it gets deleted before any purchase comes through
    System.out.println("\nExample 5 - user then delete then data\n");
    streamUserRecord("alice", "First=Alice");
    streamUserRecord("alice", null);
    streamPurchaseRecord("alice", "Apache Kafka Series (5)");
    assertNull(readJoinOutputRecord());
    assertEquals("Purchase=Apache Kafka Series (5),UserInfo=null", readLeftJoinOutputRecord().value());

  }

}
