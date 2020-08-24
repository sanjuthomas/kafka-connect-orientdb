[![Build Status](https://travis-ci.com/sanjuthomas/kafka-connect-orientdb.svg?branch=develop)](https://travis-ci.com/sanjuthomas/kafka-connect-orientdb)
[![codecov](https://codecov.io/gh/sanjuthomas/kafka-connect-orientdb/branch/develop/graph/badge.svg)](https://codecov.io/gh/sanjuthomas/kafka-connect-orientdb)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-orientdb.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-orientdb?ref=badge_shield)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/bb5f9a752e1d40b3b8017a793b0d6ce1)](https://www.codacy.com/manual/sanjuthomas/kafka-connect-orientdb?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=sanjuthomas/kafka-connect-orientdb&amp;utm_campaign=Badge_Grade)
[![Maintainability](https://api.codeclimate.com/v1/badges/477733e9184dfbadade4/maintainability)](https://codeclimate.com/github/sanjuthomas/kafka-connect-orientdb/maintainability)

# Overview
Kafka Connect OrientDB  is a sink only connector to pull messages from Kafka to store in OrientDB as JSON documents.

## Prerequisites
[Apache ZooKeeper](https://zookeeper.apache.org) and [Apache Kafka](https://kafka.apache.org) installed and running in your machine. Please refer to respective sites to download, install, and start ZooKeeper and Kafka. 

## What is OrientDB
OrientDB is an open source NoSQL database management system written in Java. It is a multi-model database, supporting graph, document, key/value, and object models, but the relationships are managed as in graph databases with direct connections between records. It supports schema-less, schema-full and schema-mixed modes. For more details about OrientDB, please refer to OrientDB offical [website.](https://orientdb.com)

## What is Apache Kafka
Apache Kafka is an open-source stream processing platform developed by the Apache Software Foundation written in Scala and Java. The project aims to provide a unified, high-throughput, low-latency platform for handling real-time data feeds. For more details, please refer to [kafka home page](https://kafka.apache.org/).

## Configuration
Please take a look at the [orientdb-sink.properties](https://github.com/sanjuthomas/kafka-connect-orientdb/blob/master/config/orientdb-sink.properties)

```
name=orientdb-sink
connector.class=com.sanjuthomas.orientdb.sink.OrientDBSinkConnector
tasks.max=10
#topics to consume from [comma separated list for multiple topics]
topics=quote_request,open_weather_data
databaseConfigFileLocation={absolute or relative location of the config files for the topic}
batch.size=256
write.retries=2
retry.back.off.seconds=1
```

Connector expects a .yml file per topic at the location given in the ```databaseConfigFileLocation```
Please take a look at the sample topic to database mapping file given [here](https://github.com/sanjuthomas/kafka-connect-orientdb/blob/develop/config/open_weather_data.yml)

```
connectionString: {OrientDB connection string. eg - remote:localhost}
database: {name of the database. If this database does not exist, the connector will create one.}
username: {username to connect to open_weather_data}
password: {pasword to connect to open_weather_data}
className: {name of the the class to which the json document to be written. If this class does not exist, the connector will create one.}
```

Please create the database in the OrientDB serer in advance. The connector will not start if the database is not present.

## Data Mapping
OrientDB can operate both in schema-full and schemaless mode. 
This Sink Connector assume that the OrientDB is operating in schemaless mode. 
Upon receiving a collection messages from the broker, 
the connector transformer would transform the message to a format that can be written to 
OrientDB document store. As of today, this connector supports JSON messages. 
I'm happy to  write support for other serialization formats such as Apache Avro. 

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

## How to deploy the connector in Kafka
This is maven project. To create an [uber](https://maven.apache.org/plugins/maven-shade-plugin/index.html) jar, execute the following maven goals.

```mvn clean compile package shade:shade install```

Copy the artifact ```kafka-connect-orientdb-0.0.1-SNAPSHOT.jar``` to kakfa_home/lib folder.

Copy the [orientdb-sink.properties](https://github.com/sanjuthomas/kafka-connect-orientdb/blob/master/config/orientdb-sink.properties) file into kafka_home/config folder. Update the content of the property file according to your environment.

Alternatively, you may keep the ```kafka-connect-orientdb-0.0.1-SNAPSHOT.jar``` in another directory and export that directory into Kafka class path before starting the connector.

## How to start connector in stand-alone mode
Open a shell prompt, move to kafka_home and execute the following.

```
bin/connect-standalone.sh config/orientdb-connect-standalone.properties config/orientdb-sink.properties
```

## How to start connector in distribute mode
Open a shell prompt, move to kafka_home and execute the following.

```
bin/connect-distributed.sh config/orientdb-connect-distributed.properties config/orientdb-sink.properties
```

## Contact
Please send a note at odb@sanju.org or create an issue in the GitHub.

## License
Please feel free to rip it apart. This is licensed using MIT license.

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-orientdb.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fsanjuthomas%2Fkafka-connect-orientdb?ref=badge_large)