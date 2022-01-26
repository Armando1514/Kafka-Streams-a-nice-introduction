# Favourite Colour

## Problem

- Given a comma delimited topic of 'userid,colour':
  - Filter out bad data,
  - Keep only colour of 'green', 'red' or 'blue'.
- Get the running count of the favourite colours overall and output this to a topic. 

**Note**: A user's favourite colour can change.

**E.g**.:

**Input**:

- stephane,blue
- JohN,green
- stePhanE,red (update here),
- Alice,red

**Result**: blue:0, green:1, red: 2.

**Note**: Before reading my solution, try to implement it by yourself.

## Solution

- The input data does not have keys, but represents updates. We should read it a KStream and extract the key.
- We should the result to kafka (better if as a log compacted topic).
- The result can now be read as a KTable (in this way the key is unique) so that updates are correctly applied.
- We can now perform an aggregation on the KTable (groupBy and then count).
- Write the result back to Kafka.

## How to run it?

```bash
#!/bin/bash
# download kafka, extract kafka in a folder and go in it

# open a shell - zookeeper is at localhost:2181
bin/zookeeper-server-start.sh config/zookeeper.properties

# open another shell - kafka is at localhost:9092
bin/kafka-server-start.sh config/server.properties

# create input topic with two partitions
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favourite-colour-input-topic

# create middle output topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic mid-favourite-colour-output-topic --config cleanup.policy=compact


# create result output topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favourite-colour-output-topic --config cleanup.policy=compact

# launch a Kafka consumer
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic favourite-colour-output-topic \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

# launch the streams application

# then produce data to it
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic favourite-colour-input-topic

# package your application as a fat jar
mvn clean package

# run your fat jar
java -jar <your jar here>.jar

# list all topics that we have in Kafka (so we can observe the internal topics)
bin/kafka-topics.sh --list --zookeeper localhost:2181

```

## 