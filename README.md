# Overview 
Kafka Connect OrientDB  is a sink only connector to pull messages from Kafka to store in OrientDB as JSON documents.

## Prerequisites
[Apache ZooKeeper](https://zookeeper.apache.org) and [Apache Kafka](https://kafka.apache.org) installed and running in your machine. Please refer to respective sites to download, install, and start ZooKeeper and Kafka. 

## What is OrientDB?


## What is Apache Kafka?
Apache Kafka is an open-source stream processing platform developed by the Apache Software Foundation written in Scala and Java. The project aims to provide a unified, high-throughput, low-latency platform for handling real-time data feeds. For more details, please refer to [kafka home page](https://kafka.apache.org/).

## Data Mapping



**For stand-alone mode**, please copy ```kafka_home/config/connect-standalone.properties``` to create ```kafka_home/config/orientdb-connect-standalone.properties``` file. Open ```kafka_home/config/orientdb-connect-standalone.properties``` and set the following properties to false.

```
key.converter.schemas.enable=false
value.converter.schemas.enable=false
```

**For distributed mode**, please copy ```kafka_home/config/connect-distributed.properties``` to create ```kafka_home/config/orientdb-connect-distributed.properties``` file. Open ```kafka_home/config/orientdb-connect-distributed.properties``` and set the following properties to false.

```
key.converter.schemas.enable=false
value.converter.schemas.enable=false
```

In distributed mode, if you run more than one worker per host, the ```rest.port``` settings must have different values for each instance. By default REST interface is available at 8083.

## How to deploy the connector in Kafka?
This is maven project. To create an [uber](https://maven.apache.org/plugins/maven-shade-plugin/index.html) jar, execute the following maven goals.

```mvn clean compile package shade:shade install```

Copy the artifact ```kafka-connect-orientdb-0.0.1-SNAPSHOT.jar``` to kakfa_home/lib folder.

Copy the [orientdb-sink.properties](https://github.com/sanjuthomas/kafka-connect-orientdb/blob/master/config/orientdb-sink.properties) file into kafka_home/config folder. Update the content of the property file according to your environment.

Alternatively, you may keep the ```kafka-connect-orientdb-0.0.1-SNAPSHOT.jar``` in another directory and export that directory into Kafka class path before starting the connector.

## How to start connector in stand-alone mode?
Open a shell prompt, move to kafka_home and execute the following.

```
bin/connect-standalone.sh config/orientdb-connect-standalone.properties config/orientdb-sink.properties
```

## How to start connector in distribute mode?
Open a shell prompt, move to kafka_home and execute the following.

```
bin/connect-distributed.sh config/orientdb-connect-distributed.properties config/orientdb-sink.properties
```

## Contact
Create an issue in the GitHub.

## License
The project is licensed under the MIT license.
