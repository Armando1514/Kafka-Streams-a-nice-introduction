# kaka-streams-example

## Kafka Streams Application Terminology

- A '**Stream**' is a sequence of immutable data records, that fully ordered, can be replayed, and this process is fault tolerant.

- A '**Stream processor**' is a node in the processor topology (graph). It transforms incoming streams, record by record, and may create a new stream from it (but can't change the stream, because it is immutable).

- A '**topology**' is a graph of processors chained together by streams.

  <img src="./graphStream.png" alt="graph showing the stream" style="zoom: 33%;" />

- A '<span style="color:green">**Source processor**</span>' is a special processor that takes its data directly from a Kafka Topic. It has no predecessors in a topology, and doesn't transform the data.
- A '<span style="color:orange">**Sink processor**</span>' is a processor that does not have children, it sends the stream data directly to a Kafka topic.

<img src="./graphTopology.png" alt="graph showing the stream" style="zoom: 33%;" />



## Streams App Properties

- A stream application, when communicating to Kafka, is leveraging the Consumer and Producer API.
- bootstrap.servers: need to connect to kafka (usually port 9092).
- auto.offset.reset.config: set to 'earliest' to consume the topic from start.
- application.id: specific to Streams application, will be used for:
  - ​	Consumer group.id = application.id (most important one to remember).
  - ​    Default client.id prefix.
  - ​    Prefix to internal changelog topics.
- default.[key| value].serde (for Serialization and Deserialization of data).

## Java 8 Lambda Functions

- **Java 7**: What you used to write when passing a function:

  ```java
  stream.filter(new Predicate<String, Long> () {
    	@Override
  		public boolean test(String key, long value) {
  			 return value > 0;
  		}
  });
  ```

- **Java 8**: Can now be written as an anonymous lambda function:

  ```java
  stream.filter((key, value) -> value > 0);
  ```

  The types of key and value are inferred at compile time.

  More Info at: [w3schools java lambda](https://www.w3schools.com/java/java_lambda.asp)

  