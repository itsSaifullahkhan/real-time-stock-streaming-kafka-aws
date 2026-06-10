# AWS MSK to Snowflake Real-Time Streaming Pipeline

## Project Overview

This project demonstrates a real-time data streaming pipeline using **AWS MSK**, **Apache Kafka**, **Snowflake Kafka Connector**, and **Snowflake**.

The pipeline streams data from Kafka topics hosted on AWS MSK and continuously loads that data into Snowflake tables for analytics, reporting, and downstream processing.

This project shows how real-time data can move from a Kafka-based streaming system into a cloud data warehouse using a production-style connector approach.

---

## Problem Statement

Modern businesses need real-time access to data for analytics, monitoring, reporting, and decision-making. Traditional batch pipelines load data after a delay, which is not suitable for use cases where fresh data is important.

The problem is to build a cloud-based real-time ingestion pipeline that can:

* Stream data continuously from Kafka topics
* Load streaming data automatically into Snowflake
* Reduce manual batch loading
* Support near real-time analytics
* Store data in a scalable cloud warehouse
* Create a reliable foundation for reporting and dashboards

This project solves the problem by connecting **AWS MSK Kafka topics** with **Snowflake** using the **Snowflake Kafka Connector**.

---

## Solution

The solution uses **AWS MSK** as the managed Kafka service and **Snowflake** as the cloud data warehouse.

Data is produced into Kafka topics. The Snowflake Kafka Connector reads data from those Kafka topics and automatically writes the streaming data into Snowflake tables.

The connector handles topic consumption, buffering, flushing, and writing data into Snowflake. This removes the need to manually export Kafka data and load it into the warehouse.

---

## Architecture

```text
Data Producer
     |
     v
AWS MSK Kafka Topic
     |
     v
Snowflake Kafka Connector
     |
     v
Snowflake Internal Stage / Pipe
     |
     v
Snowflake Target Table
     |
     v
Analytics / Reporting
```

### Architecture Diagram

![Architecture Diagram](imgs/architecture.png)

---

## Architecture Flow

### 1. Data Producer

A data producer sends records into a Kafka topic.
The producer can be:

* A Python script
* Kafka CLI producer
* Application service
* Event generator
* Real-time data source

The producer is responsible for pushing event data into the Kafka topic.

---

### 2. AWS MSK Cluster

AWS MSK is used as the managed Apache Kafka service.

It handles:

* Kafka broker management
* Topic creation
* Message storage
* Streaming data movement
* Kafka cluster operations

Using AWS MSK removes the need to manually manage Kafka infrastructure from scratch.

---

### 3. VPC and Networking

The AWS MSK cluster runs inside a VPC.
Networking configuration is important because the connector and client machines must be able to communicate with the MSK brokers.

This project includes VPC-related setup to support communication between:

* EC2 instance
* MSK cluster
* Kafka tools
* Snowflake connector

---

### 4. EC2 Instance

An EC2 instance is used for setup, configuration, and Kafka command execution.

EC2 can be used to:

* Connect with the MSK cluster
* Create or test Kafka topics
* Run Kafka CLI commands
* Manage connector configuration files
* Debug connectivity issues

---

### 5. Snowflake Kafka Connector

The Snowflake Kafka Connector consumes messages from Kafka topics and writes them into Snowflake.

The connector is responsible for:

* Reading data from Kafka topics
* Mapping topics to Snowflake tables
* Buffering records
* Flushing records into Snowflake
* Handling continuous ingestion
* Using Snowflake authentication for secure access

---

### 6. Snowflake Data Warehouse

Snowflake stores the streamed data in target tables.

After data reaches Snowflake, it can be used for:

* SQL analysis
* Reporting
* Dashboards
* Data transformations
* Downstream dbt models
* Business intelligence workflows

---

## Tech Stack

| Category              | Tool / Service             |
| --------------------- | -------------------------- |
| Streaming Platform    | Apache Kafka               |
| Managed Kafka Service | AWS MSK                    |
| Cloud Provider        | AWS                        |
| Compute               | AWS EC2                    |
| Networking            | AWS VPC                    |
| Data Warehouse        | Snowflake                  |
| Connector             | Snowflake Kafka Connector  |
| Authentication        | Private Key Authentication |
| CLI Tools             | Kafka CLI                  |
| Version Control       | GitHub                     |

---

## Key Features

* Real-time Kafka-to-Snowflake data streaming
* AWS MSK managed Kafka cluster
* Snowflake Kafka Connector integration
* Kafka topic to Snowflake table mapping
* Continuous data ingestion
* Configurable buffering and flush interval
* EC2-based setup and testing
* Secure Snowflake authentication
* Cloud-based streaming architecture
* Scalable foundation for analytics and reporting

---

## Repository Structure

```text
.
├── imgs/
│   ├── architecture.png
│   ├── msk-connector.png
│   ├── snowflake-table.png
│   └── vpc-setup.png
│
├── aws-msk-connector.txt
└── README.md
```

---

## Important File

### `aws-msk-connector.txt`

This file contains the Snowflake Kafka Connector configuration.

It includes configuration related to:

* Kafka topics
* Snowflake database
* Snowflake schema
* Snowflake table mapping
* Connector class
* Buffer size
* Flush interval
* Authentication method
* Private key configuration

Important: Do not expose real private keys, passwords, tokens, AWS credentials, or Snowflake account secrets in a public repository.

---

## Connector Configuration Overview

The Snowflake Kafka Connector configuration usually includes settings like:

```properties
connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
tasks.max=1
topics=your_kafka_topic

snowflake.url.name=your_snowflake_account_url
snowflake.user.name=your_snowflake_user
snowflake.private.key=your_private_key
snowflake.database.name=your_database
snowflake.schema.name=your_schema
snowflake.topic2table.map=your_topic:your_table

buffer.count.records=10000
buffer.flush.time=60
buffer.size.bytes=5000000
```

Note: The values above are only examples. Replace them with your own secure configuration values when running the project.

---

## How the Pipeline Works

1. A Kafka topic is created inside AWS MSK.
2. Data is produced into the Kafka topic.
3. The Snowflake Kafka Connector is configured.
4. The connector starts consuming data from the Kafka topic.
5. Records are buffered by the connector.
6. The connector flushes records into Snowflake.
7. Snowflake stores the data in the target table.
8. The data becomes available for SQL queries, dashboards, and downstream analytics.

---

## Screenshot

### Architecture Diagram

![Architecture Diagram](Architecture.jpg)

---

## Example Use Cases

This type of real-time pipeline can be used for:

* Real-time financial transaction ingestion
* Stock market event streaming
* Clickstream analytics
* IoT sensor data ingestion
* Application log streaming
* Fraud detection pipelines
* Real-time customer activity tracking
* Operational monitoring dashboards
* Real-time reporting systems

---

## Business Value

This pipeline helps businesses move from delayed batch processing to near real-time data availability.

Key business benefits include:

* Faster reporting
* Better real-time visibility
* Reduced manual data loading
* Scalable cloud-based ingestion
* Centralized storage in Snowflake
* Better support for dashboards and analytics
* Improved decision-making using fresh data

---

## Challenges Faced

Some key challenges in this project included:

* Configuring AWS MSK correctly
* Understanding MSK networking and VPC connectivity
* Connecting EC2 with the MSK cluster
* Setting up Kafka tools on EC2
* Configuring the Snowflake Kafka Connector
* Understanding topic-to-table mapping
* Setting up private key authentication for Snowflake
* Debugging connector deployment issues
* Validating whether data was successfully loaded into Snowflake

---

## What I Learned

Through this project, I learned:

* How AWS MSK works as a managed Kafka service
* How Apache Kafka topics are used in real-time pipelines
* How to connect Kafka with Snowflake
* How the Snowflake Kafka Connector works
* How topic-to-table mapping is configured
* How connector buffering and flushing work
* How Snowflake stores streamed data
* How cloud networking affects streaming pipelines
* How real-time pipelines are different from batch ETL pipelines

---

## Future Improvements

This project can be improved by adding:

* Python producer script for sample data generation
* Sample JSON event data
* Kafka consumer for testing
* Snowflake Streams and Tasks
* dbt models for downstream transformation
* Airflow orchestration
* Monitoring and logging
* Error handling and retry strategy
* Terraform for infrastructure automation
* Power BI dashboard on top of Snowflake data
* CI/CD workflow for connector configuration validation

---


## Final Result

This project successfully demonstrates a real-time streaming data pipeline from AWS MSK Kafka topics into Snowflake using the Snowflake Kafka Connector.

It shows how streaming data can be continuously ingested into a cloud data warehouse and made available for analytics, reporting, and downstream processing.

---

## Author

**Saifullah Khan**
Cloud Data Engineer | BS Financial Technology Student

* GitHub: [itsSaifullahkhan](https://github.com/itsSaifullahkhan)
* LinkedIn: [Saifullah khan](https://www.linkedin.com/in/saifullah-khan-b4616225b/)
