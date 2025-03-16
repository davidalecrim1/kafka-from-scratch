# Kafka from Scratch
Kafka from Scratch is a custom implementation of a simplified Kafka broker, built using Go. The goal is deep dive into Kafka internals and a platform to hone my Go programming skills. It demonstrates hands-on experience with distributed messaging systems, network protocols, and scalable system design.


## Project Overview
- **Deepen Go Expertise:** By building a robust, concurrent server from scratch, I’ve enhanced my understanding of Go’s concurrency patterns, memory management, and network programming.
- **Demystify Kafka Internals:** Gain a comprehensive understanding of how Kafka’s protocol, replication, and partitioning work by reconstructing them in a simplified form.
- **Hands-On Learning:** Translate theory into practice by solving real-world problems in distributed systems and network communication.


## Key Features
- **TCP Communication:** Implements the core TCP protocol used by Kafka brokers and clients for low-level message passing.
- **Custom Protocol Implementation:** Develops and parses custom binary protocols, mimicking Kafka’s message format for both requests and responses.
- **Modular Architecture:** Designed with extensibility in mind to facilitate the addition of advanced features over time.


## Getting Started

### Prerequisites
- [Go](https://golang.org/doc/install) (version 1.24 or later recommended)
- Basic knowledge of Kafka architecture and TCP networking concepts


### Installation
Clone the repository and run the broker:
```bash
make run
```

### Running Tests
A suite of tests is included to verify protocol compliance and server stability. Run them using:
```bash
make tests
```

## Documentation

### TCP Communication
The foundation of this project lies in its TCP-based communication:
- **Message Parsing:** Serialization and deserialization of Kafka-like binary messages.
- **Connection Handling:** Managing client connections, message routing, and error handling.
- **Protocol Design:** Detailed implementation of Kafka’s APIVersions, DescribeTopicPartitions, and other API calls.


## Future Roadmap

To extend the project’s capabilities, upcoming features include:
- **Consumer Groups & Offsets:** Support for multiple consumers, group coordination, and offset management.
- **Enhanced Topic Management:** Implement topic discovery, creation, and deletion functionalities.
- **Performance Optimizations:** Improvements in scalability and resource management to support larger workloads.
- **Monitoring & Logging:** Enhanced observability for debugging and performance tracking.

### Overall APIs
- [List Unknown Topic](https://app.codecrafters.io/courses/kafka/stages/vt6?repo=c1cd8e5f-4a4c-4890-8dcd-d102bbb982b7).