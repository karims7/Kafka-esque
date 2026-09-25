# Kafka-esque

A simplified Kafka-like distributed messaging system built from scratch in Java.
Built incrementally to understand how real Kafka works under the hood.

---

## What is Kafka? (The Post Office Analogy)

Kafka is a middleman message system that lets different parts of a program
talk to each other without being directly connected.

The best way to think about it is a post office inside a building:

- **Producer** — the person who drops off a letter (a message)
- **Topic** — the labeled mailbox slot (e.g. "Orders", "Payments")
- **Consumer** — the person who picks up letters from that slot
- **Broker** — the post office building itself; it manages all the slots and letters

Instead of Department A calling Department B directly (which gets messy fast),
everyone drops letters in the right slot and whoever needs that info picks it
up when they're ready.

### CRUD in Kafka

| CRUD   | Kafka Equivalent         | Who Does It        |
| ------ | ------------------------ | ------------------ |
| Create | Send a message           | Producer           |
| Read   | Consume a message        | Consumer           |
| Update | Does not exist in Kafka  | Nobody             |
| Delete | Message expiry/retention | Broker (automatic) |

Kafka intentionally has no Update. Once a message is written to the log,
it stays there. This is by design — Kafka is an append-only log, like a diary.

---

## Project Structure

```
mini-kafka/
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/
│   │           └── minikafka/
│   │               ├── broker/
│   │               └── client/
│   └── test/
│       └── java/
└── pom.xml
```

The `broker` package contains the server-side code — the post office itself.
The `client` package contains the producer and consumer — the people using the post office.
This mirrors how real Kafka separates its server and client components.

---

## Stage 1 — Project Setup

### Maven and pom.xml

Maven is a project manager tool. Instead of manually downloading libraries
and telling Java where to find them, Maven handles it automatically.

`pom.xml` is Maven's shopping list. The key sections are:

**Project identity** — who made it, what it's called, what version it is:

```xml
<groupId>com.minikafka</groupId>
<artifactId>mini-kafka</artifactId>
<version>1.0-SNAPSHOT</version>
```

**Java version** — compile and run using Java 11:

```xml
<maven.compiler.source>11</maven.compiler.source>
<maven.compiler.target>11</maven.compiler.target>
```

**Dependencies** — external libraries the project needs:

- `zookeeper` — the receptionist that tracks which brokers are alive (used in Stage 3)
- `junit-jupiter` — for writing tests later

**Maven Shade Plugin** — packages all code and libraries into one single
runnable JAR file, and declares which class contains the `main()` entry point.

---

## Stage 2 — Core Protocol Layer

### BrokerInfo.java

`BrokerInfo` is a simple data container. Every broker (post office location)
in the cluster has three pieces of identity: an ID number, a host address,
and a port number. This class holds those three things together.

Key OOP concepts used:

- **Encapsulation** — all fields are `private final`, meaning nothing outside
  this class can change them after creation
- **Getters** — the only way to read the fields from outside the class
- `equals()` — two BrokerInfo objects are considered the same if they share
  the same ID
- `hashCode()` — required whenever `equals()` is overridden in Java; used
  when BrokerInfo objects are stored in HashMaps or HashSets

### Protocol.java

The Protocol class defines how brokers and clients talk to each other over
the network. This is called the **wire protocol**.

#### Why Binary?

MiniKafka uses a binary protocol instead of something human-readable like JSON.
The reasons:

- **Smaller size** — a 4-byte integer is always 4 bytes in binary; in JSON it
  could be 1–10 characters
- **Faster parsing** — no complex string parsing needed
- **Less network usage** — smaller messages = faster transmission

#### How a Message is Structured

Every message starts with one byte that identifies what kind of message it is:

| Byte | Meaning                                  |
| ---- | ---------------------------------------- |
| 0x01 | Produce request (write a message)        |
| 0x02 | Fetch request (read messages)            |
| 0x03 | Metadata request (ask about the cluster) |
| 0x04 | Create topic request                     |
| 0x11 | Produce response                         |
| 0x12 | Fetch response                           |
| 0x15 | Error response                           |

Think of this first byte like the subject line of a letter — it tells the
receiver what kind of message to expect before reading the rest.

#### Request Encoding (Sending Messages)

- `encodeProduceRequest` — packages a message to write into a topic/partition
- `encodeFetchRequest` — packages a request to read from a topic/partition at a given offset
- `encodeMetadataRequest` — the simplest request; just one byte asking "tell me about the cluster"
- `encodeCreateTopicRequest` — packages a request to create a new topic

#### Response Decoding (Reading Replies)

- `decodeProduceResponse` — reads the broker's reply to a produce request;
  returns the offset where the message was stored
- `decodeFetchResponse` — reads the broker's reply to a fetch request;
  returns an array of messages
- `decodeMetadataResponse` — reads broker and topic information from the cluster

#### Result Classes (Nested Classes)

Protocol contains several nested classes that act as data containers for
decoded responses:

- `ProduceResult` — holds the offset and any error from a produce operation
- `FetchResult` — holds the array of messages and any error from a fetch operation
- `MetadataResult` — holds broker list, topic list, and any error
- `TopicMetadata` — holds a topic name and its partition information
- `PartitionMetadata` — holds a partition ID, its leader broker, and its replicas

#### Broker-to-Broker Communication

Two methods handle internal communication between brokers (not client-facing):

- `encodeReplicateRequest` — one broker telling another to copy a message
  (this is how Kafka keeps backups)
- `encodeTopicNotification` — one broker telling others that a new topic was created

#### Error Handling

`sendErrorResponse` gives a standardized way to send error messages back to
clients over the network using a SocketChannel.
