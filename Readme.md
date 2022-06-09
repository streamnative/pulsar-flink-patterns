<p align="center">
    <img src="images/logo.png" width="500" height="300">
</p>

### Environment Setup
In order to run the code samples we will need a Pulsar and Flink cluster up and running.
You can also run the Flink examples from within your favorite IDE in which case you don't need a Flink Cluster.

If you want to run the examples inside a Flink Cluster run to start the Pulsar and Flink clusters.
```shell
docker-compose up
```

if you want to run within your IDE you can just spin up a pulsar cluster
```shell
docker-compose up pulsar
```

When the cluster is up and running successfully run the following command:
```shell
./setup.sh
```

this script will create all the required topics and policies needed for the provided examples.

### Table of Contents
1. [Warmup - Connecting to Pulsar](#warmup)
2. [Connecting Streams](#connecting-streams)
3. Buffering Events
4. Connecting Streams
5. Performing Data Enrichment and Lookups
6. Using Side Outputs to handle "unhappy paths"
7. Flink State Backends
8. Checkpoints, Restart Strategies and Savepoints

### Warmup
1. Connect to Pulsar and consumer data:
   - Using the Datastream API
   - Using the Flink SQL API

### Connecting Streams
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