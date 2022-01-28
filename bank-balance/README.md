# Bank balance

1. Create a Kafka Producer that outputs ⁓10 messages per seconds to a topic. Each message is random in money ( a positive value), and outputs evenly transactios for 6 customers. The data should like this:

   {"name": "John", "amount": 123, "time": "2017-08-03T03:51:06.628Z"}

2. Create a Kafka Streams application that takes these transactions and will compute the total money in their balance (the balance starts at $0), and the latest time an update was received. As you guessed, the total money is not idempotent (sum), but latest time is (max).

## How to run it?

```bash
#!/bin/bash
# download kafka, extract kafka in a folder and go in it

# open a shell - zookeeper is at localhost:2181
bin/zookeeper-server-start.sh config/zookeeper.properties

# open another shell - kafka is at localhost:9092
bin/kafka-server-start.sh config/server.properties

# create input topic with two partitions
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic bank-transactions-input

# create output topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic bank-transactions-output 

# launch the producer (BankTransactionproducer)

# launch the streams application (BankTransactionsStreamApplication)

# then produce data to it
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic bank-transactions-input

# package your application as a fat jar
mvn clean package

# run your fat jar
java -jar <your jar here>.jar


```

