#Docker Instructions

## These docker instructions are prerequisites to running the programs.

Navigate locally into the directory Docker. Open a terminal.
In windows open a PowerShell and navigate into the directory

There run ```docker-compose up -d```

Docker will downloaded the required images from the docker-compose file and setup the containers

After the setup is done the Kafka connector and the Kafka clients jars need to be copyed into the jobmanager and the taskamanger.

Download the files from the official Maven Repository

1. flink-connector-kafka-4.0.1-2.0.jar  from https://mvnrepository.com/artifact/org.apache.flink/flink-connector-kafka/4.0.1-2.0

2. kafka-clients-3.9.1.jar   from https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/3.9.1


After downloading the jars move to their directory and copy them into the docker containers in opt/flink/lib

- Docker cp flink-connector-kafka-4.0.1-2.0.jar taskmanager200:/opt/flink/lib
- Docker cp kafka-clients-3.9.1.jar taskmanager200:/opt/flink/lib



- Docker cp flink-connector-kafka-4.0.1-2.0.jar jobmanager200:/opt/flink/lib
- Docker cp kafka-clients-3.9.1.jar jobmanager200:/opt/flink/lib


After copying the jars move to the [next step](../FlinkSQL/FlinkSQLInstructions.md), namely the Flink SQL.