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
3. [Warmup - Keyed State](#keyed-state)
4. [Performing Data Enrichment and Lookups](#data-enrichment-and-lookups)
5. [Side Outputs](#side-outputs)
6. [State Backends](#state-backends)
7. [Checkpoints](#checkpoints)

### Warmup
Connect to Pulsar and consumer data:
   - Using the Datastream API
   - Using the Flink SQL API

### Connecting Streams
**Use Case**

* Using the Union Function
  * Input datastreams need to be of the same input data type
* Using the Connect Functions
  * Allows the input datastreams to be of different types

### Keyed State
**Use Case**

### Data Enrichment and Lookups
**Use Case**

### Side Outputs
**Use Case**

### State Backends
**Use Case**

### Checkpoints
**Use Case**
