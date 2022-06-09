<p align="center">
    <img src="images/logo.png" width="500" height="300">
</p>

### Environment Setup
1. Cluster Setup
```shell
docker-compose up
```

2. Setup Topics and Policies
```shell
./setup.sh
```

### Outline
1. Pulsar Flink Connector - Datastream API
2. Pulsar Flink Connector - SQL API
3. Consuming Multiple Streams
4. Buffering Events
5. Connecting Streams
6. Performing Data Enrichment and Lookups
7. Using Side Outputs to handle "unhappy paths"
8. Flink State Backends
9. Checkpoints, Restart Strategies and Savepoints

### Warmup
1. Connect to Pulsar and consumer data:
   - Using the Datastream API
   - Using the Flink SQL API

### Combine data from Multiple Topics
Use Case:

* Using the Union Function
  * Input datastreams need to be of the same input data type
* Using the Connect Functions
  * Allows the input datastreams to be of different types

### Buffering events
**Use Case:** You are consuming data from multiple topics and you need wait for events in both topics to arrive

### Keyed State Warmup
State Types


### 
State Types
===========
1. Operator State
   1. private to an operator task
   2. can't be read/written by another operator task
2. Keyed State
   1. one instance per key
3. About keys
   1. a piece of data that is used to group events
   2. multiple events can be assigned to the same key
   3. an event can be assigned to a single key
   4. examples of keys
      1. hash code of a String field of the event
      2. a number field of the event, mod 10
      3. a number event type (enum)