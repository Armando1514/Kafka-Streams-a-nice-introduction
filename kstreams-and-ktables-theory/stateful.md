# KStreams-and-KTables Advance Operations

### KTable GroupBy

GroupBy allows you to perform more aggregations within a KTable. It triggers a repartition because the key changes.

```java
// Group the table by a new key and key type
KGrupedTable<String, Integer> groupedTable = table.groupBy(
(key, value) -> KeyValue.pair(value, value.length()),
Serdes.String(),
Serdes.Integer());
```

## KGroupedStream/ KGroupedTable count

As a reminder, KGroupedStream are obtained after a 'groupBy/ groupByKey()' call on a KStream.

Count counts the number of record by grouped key.

- If used on KGroupedStream: null keys or values are ignored.
- If used on KGroupedTable:
  - Null keys are ignored.
  - Null values are treated as 'delete' (tombstones).

## KGroupedStream Aggregate

You need an initializer (of any type), an adder, a Serde and a state store name (name of your aggregation). E.g. Count total String length by key.

```java
// Aggregating a KGroupedStream (note how the value type changes from String to Long)
KTable<byte[], Long> aggregatedStream = groupedStream.aggregate(
() -> 0L, //initializer, the first Value when we start the aggregate is 0
(aggKey, newValue, aggValue) -> aggValue + newValue.length(),//adder, aggValue is the current value and we sum the new value length coming from the stream
Serdes.Long(),//serde for aggregate type value
"aggregated-stream-store"//state store name
);
```

## KGroupedTable Aggregate

You need an initializer (of any type), an adder, a <u>subtractor</u> (because data from KTable can be deleted and we need to know what operation to perfrom what this happen), a Serde and a State Store name (name of your aggregation). E.g. Count total String length by key.

```java
// Aggregating a KGroupedStream (note how the value type changes from String to Long)
KTable<byte[], Long> aggregatedStream = groupedStream.aggregate(
() -> 0L, //initializer, the first Value when we start the aggregate is 0
(aggKey, newValue, aggValue) -> aggValue + newValue.length(),//adder, aggValue is the current value and we sum the new value length coming from the stream,
(aggKey, oldValue, aggValue) -> aggValue -oldValue.length(),//subtractor, aggValue is the current value and we subtract the new old value length, because it has been removed. 
Serdes.Long(),//serde for aggregate type value
"aggregated-stream-store"//state store name
);
```

## KGroupedStream/ KGroupedTable Reduce

It is a simplified version of the aggregate transformation. It is similar to Aggregate, but the result type has to be the same as the input (Int, Int => Int, can't be a Long for example).

E.g.

```java

KTable<String, Long> aggregatedStream = groupedStream.reduce(
(aggValue, newValue) -> aggValue + newValue.length(),
"reduced-stream-store"
);
```

```java
KTable<String, Long> aggregatedStream = groupedTable.reduce(
(aggValue, newValue) -> aggValue + newValue,
(aggValue, oldValue) -> aggValuie - oldValue.length(),
"reduced-stream-store"
);
```

## KStream Peek

Peek allows you to apply a side-effect (modify an external element but not the Stream itself) operation to a KStream and get the same KStream as a result. A side-effect could be for instance printing the stream to the console or statistics collection. 

**Note**: It could be executed multiple (in case of failure), times as it is side effect, can lead to duplicate messages. 

E.g.

```java
KStream<byte[], String> stream = ...;
KStream<byte[], String> unmodifiedStream = stream.peek( (key,value) -> System.out.println('key='+ key + ", value=" + value));
```

## 
