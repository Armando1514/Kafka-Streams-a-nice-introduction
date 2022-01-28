package com.github.kafka.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class UserDataProducer {

  public static void main(String[] args) {
    Properties config = new Properties();

    config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    config.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    config.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
    config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
    // leverage idempotent producer
    config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

    KafkaProducer<String, String> producer = new KafkaProducer<>(config);

    // we are going to test different scenarios to illustrate the join

    try {
      // 1 - we create a new user, then we send some data to Kafka
      System.out.println("\nExample 1 - new user\n");
      producer.send(userRecord("john", "First=John,Last=Doe,Email=john.doe@gmail.com")).get();
      producer.send(purchaseRecord("john", "Apples and Bananas (1)")).get();

      Thread.sleep(10000);

      // 2 - we receive user purchase, but it doesn't exist in Kafka
      System.out.println("\nExample 2 - non existing user\n");
      producer.send(purchaseRecord("bob", "Kafka Udemy Course (2)")).get();

      Thread.sleep(10000);

      // 3 - we update user "john", and send a new transaction
      System.out.println("\nExample 3 - update to user\n");
      producer.send(userRecord("john", "First=Johnny,Last=Doe,Email=johnny.doe@gmail.com")).get();
      producer.send(purchaseRecord("john", "Oranges (3)")).get();

      Thread.sleep(10000);

      // 4 - we send a user purchase for stephane, but it exists in Kafka later
      System.out.println("\nExample 4 - non existing user then user\n");
      producer.send(purchaseRecord("stephane", "Computer (4)")).get();
      producer.send(userRecord("stephane", "First=Stephane,Last=Maarek,GitHub=simplesteph")).     get();
      producer.send(purchaseRecord("stephane", "Books (4)")).get();
      producer.send(userRecord("stephane", null)).get(); // delete for cleanup

      Thread.sleep(10000);

      // 5 - we create a user, but it gets deleted before any purchase comes through
      System.out.println("\nExample 5 - user then delete then data\n");
      producer.send(userRecord("alice", "First=Alice")).get();
      producer.send(userRecord("alice", null)).get(); // that's the delete record
      producer.send(purchaseRecord("alice", "Apache Kafka Series (5)")).get();

      Thread.sleep(10000);
    } catch (Exception e) {
      e.printStackTrace();
      producer.close();
    }

    System.out.println("End of demo");
    producer.close();

  }

  private static ProducerRecord<String, String> userRecord(String key, String value){
    return new ProducerRecord<>("user-table", key, value);
  }


  private static ProducerRecord<String, String> purchaseRecord(String key, String value){
    return new ProducerRecord<>("purchases", key, value);
  }

}
