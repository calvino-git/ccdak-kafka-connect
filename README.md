# CCDAK Kafka Producer and Cluster

A Java-based Kafka producer application for the Confluent Certified Developer for Apache Kafka (CCDAK) exam preparation. This project demonstrates how to build a Kafka producer using the Kafka Java client library.

## Project Overview

This project implements a Kafka producer that:
- Connects to a Kafka cluster
- Sends 200 messages (incrementing from 0 to 1000 in steps of 5) to the `streams-input-topic` topic
- Uses idempotent producer configuration to ensure exactly-once semantics
- Implements retry logic and error handling

## Prerequisites

- **Java**: JDK 8 or higher
- **Gradle**: 7.5.1 or higher (or use the included Gradle wrapper)
- **Kafka**: Running Kafka cluster (default: `localhost:9092`)

## Project Structure

```
.
├── build.gradle              # Gradle build configuration
├── settings.gradle           # Gradle settings
├── src/
│   ├── main/java/
│   │   └── com/linuxacademy/ccdak/kafkaJavaConnect/
│   │       └── Main.java     # Kafka producer implementation
│   └── test/
├── gradle/                   # Gradle wrapper files
├── gradlew                   # Gradle wrapper (Linux/macOS)
├── gradlew.bat              # Gradle wrapper (Windows)
└── README.md                # This file
```

## Dependencies

- **Kafka Clients**: `2.2.1` - Apache Kafka Java client library
- **Apache Commons Lang3**: `3.14.0` - For random string generation

## Configuration

The producer is configured with the following properties in `Main.java`:

| Property | Value | Description |
|----------|-------|-------------|
| `bootstrap.servers` | `127.0.0.1:9092` | Kafka cluster bootstrap servers |
| `key.serializer` | `StringSerializer` | Serializer for message keys |
| `value.serializer` | `StringSerializer` | Serializer for message values |
| `max.block.ms` | `5000` | Max time to block for send operations |
| `retries` | `3` | Number of retries on failed sends |
| `enable.idempotence` | `true` | Enable exactly-once semantics |

## How to Run

### 1. Start a Kafka Cluster

If you don't have a Kafka cluster running, you can start one using Docker:
use Docker Compose file `docker-compose.yaml`:
start with: `docker compose up -d`

### 2. Create the Topic (if it doesn't exist)

```bash
docker exec kafka1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic streams-input-topic \
  --partitions 5 \
  --replication-factor 3
  --property min.insync.replicas=2
```

### 3. Build the Project

There's a file `.sdkmanrc` that helps to automatically set JAVA_HOME into java 8 home folder. 

Using Gradle wrapper:

```bash
./gradlew build
```

Or with Gradle directly:

```bash
gradle build
```

### 4. Run the Producer

Using Gradle wrapper:

```bash
./gradlew run
```

Or with Gradle directly:

```bash
gradle run
```

Or run the JAR directly:

```bash
java -cp build/libs/content-ccdak-kafka-java-connect.jar com.linuxacademy.ccdak.kafkaJavaConnect.Main
```

### Expected Output

The producer will send 200 messages and output:

```
0->A
RecordMetadata(...)
5->B
RecordMetadata(...)
...
```

Each line shows:
- The message key (incrementing from 0 to 1000 in steps of 5)
- A random alphabetic character
- Metadata about the sent message (topic, partition, offset, timestamp)

## Monitoring Messages

To consume and view the messages being produced:

```bash
docker exec kafka1 kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic streams-input-topic \
  --from-beginning
```

## Customization

To modify the producer behavior, edit `src/main/java/com/linuxacademy/ccdak/kafkaJavaConnect/Main.java`:

- **Change bootstrap servers**: Modify `BOOTSTRAP_SERVERS_CONFIG`
- **Change topic name**: Update `"streams-input-topic"` in `new ProducerRecord()`
- **Change message count**: Modify the loop condition `i < 1000`
- **Change message interval**: Modify `Thread.sleep(1000)` (in milliseconds)
- **Adjust retry policy**: Modify `RETRIES_CONFIG` and `MAX_BLOCK_MS_CONFIG`

## Troubleshooting

### Connection Refused Error

- Ensure Kafka is running and accessible on `localhost:9092`
- Check firewall rules
- Verify the bootstrap server configuration

### Topic Not Found Error

- Create the topic before running the producer
- Use the "Create the Topic" command above

### Build Fails

- Ensure Java 8+ is installed: `java -version`
- Ensure Gradle 5.5.1+ is available: `gradle --version`

## License

This project is part of the Confluent CCDAK training materials.

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Kafka Clients](https://docs.confluent.io/kafka-clients/java/current/overview.html)
- [CCDAK Exam](https://www.confluent.io/certification/)
