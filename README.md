# Scale Your DB with kafka-connect? Part 1

If I had a dollar for every time I complain about the legacy database the
company has been using being the bottleneck of the whole system... The aim 
of this project is to show a practical way to scale the persistence layer of
multi-moduled systems with kafka-connect. As "microservices architectures" rise
with their impacable glory, the amount of time spent on the cable has shared 
the same glory with its creator. Consecutively, applications' being close to
the data has become more and more significant. In-memory for the rescue right.

The things that I applied for this project may not be suitable for certain
cases, and by "certain" I mean "most" of the enterprise solutions, but they
were fun to implement anyway.

Before going into more detail about this project let me talk a little about
kafka-connect and leave some links to their official documentations, because
surely their explanation will be better than mine.

## What is kafka-connect?

In essence, kafka-connect is a "connector" tool between Apache Kafka and other
data systems. It can ingest databases to Kafka topics as a "source" connector or 
as a "sink" connector it can feed other data systems such as Elasticsearch. You
can think of it as an ETL tool with low latency, high scalibility and simple
extensibility, so basically you can't think of it as an ETL tool.

Here is the [official documents](https://docs.confluent.io/platform/current/connect/index.html)
by Confluent for more details.
And of course an introductory [video](https://www.youtube.com/watch?v=J6adhl3wEj4)
for the visual learners.

## Project Recipe

What I wanted to achieve with this project is basically to implement a pipeline
between a relational database system and a REST application. It will be a
step by step tutorial for each implementation, because although there are 
official documentations for every technical complexity in web, you
deeply know that they won't ever work at the first try. Here are the
components:

* postgres-db:      Persistance layer. 
* kafka:            Connector will ingest data from postgres into kafka topics.
* kafka-connect:    PostgresSQL Source connector by Debezium. ([List](https://docs.confluent.io/home/connect/self-managed/kafka_connectors.html) 
of self-managed connectors for kafka-connect.) 
* crypto-aggr:      Aggregator application (using javalin) that will populate
the database.
* crypto-connect:   REST application (using javalin) with in-memory database
fed with kafka. 

Other than these base components, there are several other instances that have
to be run with the rest in order the whole cluster to be worked properly.
At the end, all the components will be run with single docker-compose file.
Let's first write a proper docker-compose file for kafka-connect and postgres.

## Proper docker-compose File for kafka-connect and postgres That Works

```yml
version: '3.1'
services:
  db:
    image: postgres
    container_name: postgres
    restart: always
    environment:
      POSTGRES_PASSWORD: example
    ports:
      - 5432:5432
    volumes:
      - ./data:/var/lib/postgresql/data
    command:
      - postgres
      - -c
      - wal_level=logical

  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-enterprise-kafka:latest
    container_name: kafka
    depends_on:
      - zookeeper
    ports:
      - 9092:9092
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 100
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
  
  schema-registry:
    image: confluentinc/cp-schema-registry:latest
    container_name: schema-registry
    ports:
      - 8081:8081
    depends_on:
      - zookeeper
      - kafka
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL: zookeeper:2181
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: PLAINTEXT://kafka:29092

  kafka-connect:
    image: confluentinc/cp-kafka-connect:latest
    container_name: kafka-connect
    depends_on:
      - zookeeper
      - kafka
      - schema-registry
    ports:
      - 8083:8083
    environment:
      CONNECT_BOOTSTRAP_SERVERS: "kafka:29092"
      CONNECT_REST_PORT: 8083
      CONNECT_REST_ADVERTISED_HOST_NAME: "kafka-connect"
      CONNECT_GROUP_ID: compose-connect-group
      CONNECT_CONFIG_STORAGE_TOPIC: docker-connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: docker-connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: docker-connect-status
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: io.confluent.connect.avro.AvroConverter
      CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL: 'http://schema-registry:8081'
      CONNECT_LOG4J_ROOT_LOGLEVEL: "INFO"
      CONNECT_LOG4J_LOGGERS: "org.apache.kafka.connect.runtime.rest=WARN,org.reflections=ERROR"
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: "1"
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: "1"
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: "1"
      CONNECT_PRODUCER_CONFIG_ACKS: 1
      CONNECT_PLUGIN_PATH: '/usr/share/java,/usr/share/confluent-hub-components'
    command:
      - /bin/bash
      - -c
      - |
        echo "Installing Connector"
        confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest
        confluent-hub install --no-prompt debezium/debezium-connector-postgresql:1.9.3
        /etc/confluent/docker/run &
        sleep infinity

  ksqldb:
  image: confluentinc/ksqldb-server:latest
  container_name: ksqldb
  depends_on:
    - kafka
    - schema-registry
  ports:
    - 8088:8088
  environment:
    KSQL_LISTENERS: http://0.0.0.0:8088
    KSQL_BOOTSTRAP_SERVERS: kafka:29092
    KSQL_KSQL_LOGGING_PROCESSING_STREAM_AUTO_CREATE: "true"
    KSQL_KSQL_LOGGING_PROCESSING_TOPIC_AUTO_CREATE: "true"
    KSQL_KSQL_SCHEMA_REGISTRY_URL: http://schema-registry:8081
    KSQL_STREAMS_PRODUCER_MAX_BLOCK_MS: 9223372036854775807
    KSQL_KSQL_CONNECT_URL: http://kafka-connect:8083
    KSQL_KSQL_SERVICE_ID: crypto_ksql
    KSQL_KSQL_HIDDEN_TOPICS: '^_.*'
```
It will get longer with the 2 additional applications, but for now I will
explain each container in this docker-compose file.

* db: As I mentioned before, I will use postgres as the main database. Default
username for postgres image is **postgres**. In order not to lose the data
at each execution I will assign a volume under **volumes** section. 
That will create a **data** folder in the current directory and persist data.
Another important point to mention is the **wal_level** setting command.
kafka-connect listens (write-ahead loggings) to detect any data change in
database tables and using those log entries creates topic messages for
individual topics. For more detail about the [wal_level settings for postgres](https://postgresqlco.nf/doc/en/param/wal_level/).

* zookeeper: This is a coordination service that will be used to manage kafka.
(Synchronozation and group services.)

* kafka: This is the container that will run the kafka broker. Basic settings 
are applied through environment variables. **KAFKA_AUTO_CREATE_TOPICS_ENABLE**
variable should be set to **true** for automatic topic creation by
kafka-connect.

* schema-registry: Schema API that will be utilized by kafka-connect as well as
our kafka client inside crypto-connect javalin app. Basic functionality of this
service is to be a serving layer for our data schemas. For more detail about
[schema registry](https://docs.confluent.io/platform/current/schema-registry/index.html)

* kafka-connect: And finally, our queen, khaleesi, mother of changesets... In
order to run this container properly, I had to try lots of different settings
and modifications, but most of these environment variables can be found in
the official documentations. As a Key Converter I used basic String converter,
but to represent values I choose the AVRO format, because why not. Another
important thing to mention is the plugin installations. As I mentioned earlier,
you need proper connectors for different data systems (in this case postgres).
With the **command** section, we install the proper plugin (debezium-connector)
when the container boot up (packages that are installed via confluent-hub
command will go to the **/usr/share/confluent-hub-components** directory.).
**CONNECT_PLUGIN_PATH** should be also indicated with the proper directories
delimited by comma.

* ksqldb: We will use ksqldb just as an observer to our kafka topics. Normally
ksqldb is a much more powerful tool for stream processing applications. For 
more information about [ksqldb](https://ksqldb.io/overview.html).

You can find this docker-compose file with the name
"docker-compose-crypto-utils.yml" in the repository. Let's run it.

```bash
docker-compose --file docker-compose-crypto-utils.yml up -d
```

## Creating a database and a table in postgres using psql

First let's connect to our postgres database and create a table. You can attach
to the postgres container and start a **psql** session using our default user
**postgres** with the following docker command. (For further detail about [psql](https://www.postgresql.org/docs/current/app-psql.html))

```bash
docker exec -it postgres psql -U postgres
```

You can list the existing databases with the following command which for now
only lists the default databases.
```bash
\l
```

In order to create a database to work with execute the following. This will
create a database named **pg_dev** whose owner is our default **postgres**
user.

```sql
CREATE DATABASE pg_dev OWNER postgres;
```

Now let's connect to our newly created database and create a table called
**mini_ticker** with the corresponding fields. (I will explain the mini_ticker
model in the following sections. For now, I just wanted our messaging and
persistence layers to be fully available, before getting into the development
of our web applications)

```sql
\c pg_dev
```

```sql
CREATE TABLE mini_ticker (
	id serial PRIMARY KEY,
	current_time_millis BIGINT,
	epoch_time BIGINT,
	pair VARCHAR(10),
	close VARCHAR(20),
	open VARCHAR(20),
	volume VARCHAR(20));
```
After succesfully running the create script, you should see your table when you
execute the following command.

```
\dt
```
And finally let's insert a dummy row.

```sql
INSERT INTO mini_ticker(current_time_millis, epoch_time, pair, close, open, volume)
VALUES (9999, 9999, 'BTCUSDT', '10000', '10001', '10002');
```

## Connecting kafka-connect to our database
Now that we have a proper database and a table, we can connect our
kafka-connect instance to our database and start to listen change events. But
before doing that let's first check current state in our kafka instance
through ksqldb. We can attach to ksqldb container and run **ksql** CLI tool
with the following docker command.

```bash
docker exec -it ksqldb ksql http://localhost:8088
```

Inside the CLI, in order to see the current topics in our kafka instance we
can run the following:

```bash
show topics;
```

It should return something like this:
```
 Kafka Topic                    | Partitions | Partition Replicas 
------------------------------------------------------------------
 crypto-ksqlksql_processing_log | 1          | 1                  
 docker-connect-configs         | 1          | 1                  
 docker-connect-offsets         | 25         | 1                  
 docker-connect-status          | 5          | 1                  
------------------------------------------------------------------
```
These are the default topics that are created by kafka-connect and ksqldb
instances.

Now let's exit from CLI tool with
```
exit;
```
command and connect our kafka-connect instance to our database.

## curl for kafka-connect with debezium postgres connector
kafka-connect has an API that we can communicate through HTTP requests, so in
order create a connection between kafka-connect and postgres database we can
simply run the following curl command in our terminal.

```bash
curl -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '
{
    "name": "pg-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "plugin.name": "pgoutput",
      "database.server.name": "pg_dev",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "example",
      "database.dbname": "pg_dev",
      "topic.creation.default.replication.factor": 1,
      "topic.creation.default.partitions": 1,
      "poll.interval.ms": 1,
      "table.include.list": "public.(.*)",
      "heartbeat.interval.ms": "5000",
      "slot.name": "dbname_debezium",
      "publication.name": "dbname_publication",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "true",
      "transforms.unwrap.delete.handling.mode": "rewrite",
      "transforms.unwrap.add.fields": "source.ts_ms"
    }
}'
```

Inside the **config** tag, you can see the necessary configuration for the
kafka-connect source connector. You can see the details of the configurations
at the [debezium's official documentations](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).

After executing POST curl, we can check if we succesfully create a connector
with the following curl command.

```bash
curl -s "http://localhost:8083/connectors?expand=info&expand=status"
```

It should return something like the following
```json
{"pg-connector":{"info":{"name":"pg-connector","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.user":"postgres","database.dbname":"pg_dev","transforms.unwrap.delete.handling.mode":"rewrite","topic.creation.default.partitions":"1","slot.name":"dbname_debezium","publication.name":"dbname_publication","transforms":"unwrap","database.server.name":"pg_dev","heartbeat.interval.ms":"5000","plugin.name":"pgoutput","database.port":"5432","database.hostname":"postgres","database.password":"example","poll.interval.ms":"1","transforms.unwrap.drop.tombstones":"true","topic.creation.default.replication.factor":"1","name":"pg-connector","transforms.unwrap.add.fields":"source.ts_ms","transforms.unwrap.type":"io.debezium.transforms.ExtractNewRecordState","table.include.list":"public.(.*)"},"tasks":[{"connector":"pg-connector","task":0}],"type":"source"},"status":{"name":"pg-connector","connector":{"state":"RUNNING","worker_id":"kafka-connect:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"kafka-connect:8083"}],"type":"source"}}}%  
```
We can see that we have a connector called **pg-connector** and the state of it
is **RUNNING**. GREAT!!!

Now let's check the current state of our kafka instance. Connect to ksqldb
container and run ***show topics;*** command again.

```
 Kafka Topic                    | Partitions | Partition Replicas 
------------------------------------------------------------------
 crypto-ksqlksql_processing_log | 1          | 1                  
 docker-connect-configs         | 1          | 1                  
 docker-connect-offsets         | 25         | 1                  
 docker-connect-status          | 5          | 1                  
 pg_dev.public.mini_ticker      | 1          | 1                  
------------------------------------------------------------------
```

Wow, we have a new topic with the name of our new table. Let's check the events
inside this topic with the following command.

```sql
PRINT 'pg_dev.public.mini_ticker' FROM BEGINNING;
```

You should see something like the following.
```
Key format: HOPPING(KAFKA_INT) or TUMBLING(KAFKA_INT) or HOPPING(KAFKA_STRING) or TUMBLING(KAFKA_STRING) or KAFKA_STRING
Value format: AVRO
rowtime: 2022/08/08 17:33:13.066 Z, key: [1400140405@7166488599636816253/-], value: {"id": 1, "current_time_millis": 9999, "epoch_time": 9999, "pair": "BTCUSDT", "close": "10000", "open": "10001", "volume": "10002", "__source_ts_ms": 1659979992956, "__deleted": "false"}, partition: 0
```

This event is actually our first INSERT operation to the mini_ticker table.
Let's insert another row and see the change inside the kafka topic.
```
Key format: HOPPING(KAFKA_INT) or TUMBLING(KAFKA_INT) or HOPPING(KAFKA_STRING) or TUMBLING(KAFKA_STRING) or KAFKA_STRING
Value format: AVRO
rowtime: 2022/08/08 17:33:13.066 Z, key: [1400140405@7166488599636816253/-], value: {"id": 1, "current_time_millis": 9999, "epoch_time": 9999, "pair": "BTCUSDT", "close": "10000", "open": "10001", "volume": "10002", "__source_ts_ms": 1659979992956, "__deleted": "false"}, partition: 0
rowtime: 2022/08/08 17:54:01.767 Z, key: [1400140405@7166488599636816509/-], value: {"id": 2, "current_time_millis": 9998, "epoch_time": 9998, "pair": "ETHUSDT", "close": "12345", "open": "12345", "volume": "12345", "__source_ts_ms": 1659981241755, "__deleted": "false"}, partition: 0
```
As we can see, each INSERT operation creates a new entry inside the topic.
What about the DELETE you may ask. Let's delete the first row.
```sql
DELETE FROM mini_ticker WHERE ID = 1;
```

After the DELETE operation our kafka topic looks like the following.
```
Key format: HOPPING(KAFKA_INT) or TUMBLING(KAFKA_INT) or HOPPING(KAFKA_STRING) or TUMBLING(KAFKA_STRING) or KAFKA_STRING
Value format: AVRO
rowtime: 2022/08/08 17:33:13.066 Z, key: [1400140405@7166488599636816253/-], value: {"id": 1, "current_time_millis": 9999, "epoch_time": 9999, "pair": "BTCUSDT", "close": "10000", "open": "10001", "volume": "10002", "__source_ts_ms": 1659979992956, "__deleted": "false"}, partition: 0
rowtime: 2022/08/08 17:54:01.767 Z, key: [1400140405@7166488599636816509/-], value: {"id": 2, "current_time_millis": 9998, "epoch_time": 9998, "pair": "ETHUSDT", "close": "12345", "open": "12345", "volume": "12345", "__source_ts_ms": 1659981241755, "__deleted": "false"}, partition: 0
rowtime: 2022/08/08 17:56:58.509 Z, key: [1400140405@7166488599636816253/-], value: {"id": 1, "cu
rrent_time_millis": null, "epoch_time": null, "pair": null, "close": null, "open": null, "volume"
: null, "__source_ts_ms": 1659981418499, "__deleted": "true"}, partition: 0
```

As we can see, we have another event with true **__deleted** field in
the message queue. By default, debezium connector removes the records for
DELETE operations from the event stream but the way we configure the
connector prevents this behavior. (When we create the connector
with the first curl command we set
**transforms.unwrap.delete.handling.mode** to **rewrite**) This way we
have a single **source of truth** that we can use to replicate our database.

Now that we have a working kafka-connect setup, in the [second](#) part we will
write the two "microservices".

# Scale Your DB with kafka-connect? Part 2

In the first part of this series, we have created a working kafka-connect
environment running on bunch of docker containers. Although we have shown that
our database operations mirrors on specific kafka topics with an immutable
series of events, we didn't actually see the database "replication".

In this part, we are going to build 2 "micro-services" using javalin framework.
Why [javalin](https://javalin.io/) you may ask? It's simple, lightweight and
batteries not included. When it comes to any "framework", I liked to use the
ones that makes batteries available, but makes me choose which one I like to
use. javalin is just like what I want.

First service that we will build is actually going to populate our database. As
you may remember, we've created a table called **mini_ticker** in the first
part. Well, there was a reason for that. To keep things interesting, with the
service that we will build, we are going to connect to **binance**'s websocket
and listen for **mini_ticker** stream for individual crypto pairs and save each
websocket message to our **mini_ticker** table.

As we've seen at the end of the first part, each operation will create an event
in our **pg_dev.public.mini_ticker** topic. Listening these events, the second
service that we will build, is going to populate its own in-memory database (we
will use H2). This way we will "replicate" our original database, inside
individual micro-service.

## Creating a javalin application that listens a websocket

1) First we create a new Java application called crypto-aggr which uses Maven as build system.

2) We will add our dependencies.

```xml
<properties>
    <javalin-version>4.6.4</javalin-version>
    <postgresql-version>42.4.1</postgresql-version>
    <HikariCP-version>5.0.1</HikariCP-version>
    <websocket-client-version>9.4.48.v20220622</websocket-client-version>
    <jackson-databind-version>2.13.3</jackson-databind-version>
    <slf4j-simple-version>1.7.36</slf4j-simple-version>
</properties>

<dependencies>

    <dependency>
        <groupId>io.javalin</groupId>
        <artifactId>javalin</artifactId>
        <version>${javalin-version}</version>
    </dependency>

    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <version>${postgresql-version}</version>
    </dependency>

    <dependency>
        <groupId>com.zaxxer</groupId>
        <artifactId>HikariCP</artifactId>
        <version>${HikariCP-version}</version>
    </dependency>

    <dependency>
        <groupId>org.eclipse.jetty.websocket</groupId>
        <artifactId>websocket-client</artifactId>
        <version>${websocket-client-version}</version>
    </dependency>

    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
        <version>${jackson-databind-version}</version>
    </dependency>

    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-simple</artifactId>
        <version>${slf4j-simple-version}</version>
    </dependency>

</dependencies>
```

3) We are going to need 3 utility classes: ConnectionPool, Client and Socket.

### ConnectionPool.java
```java
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionPool {

	private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

	private static HikariDataSource ds;
	static {
		try(InputStream input = Files.newInputStream(Paths.get("src/main/resources/datasource.properties"))) {
			// Configure Hikari DataSource
			Properties prop = new Properties();
			prop.load(input);
			HikariConfig config = new HikariConfig(prop);
			ds = new HikariDataSource(config);

		} catch (IOException e) {
			throw new RuntimeException();
		}
	}

	public static Connection getConnection() throws SQLException {
		return ds.getConnection();
	}

	private ConnectionPool() {}
}
```
ConnectionPool.java class will handle our database connections. We will
configure the **HikariDataSource** with our **datasource.properties** file.

### datasource.properties file for local postgres with Hikari Connection Pool
```properties
jdbcUrl=jdbc:postgresql://localhost:5432/pg_dev
username=postgres
password=example
connectionTimeout=5000
maxLifetime=30000
maximumPoolSize=10
```

### Socket.java
```java
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;

@WebSocket
public class Socket {

	private Session session;
	private static final Logger logger = LoggerFactory.getLogger(Socket.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();
	private final CountDownLatch latch = new CountDownLatch(1);

	@OnWebSocketMessage
	public void onText(Session session, String message) {
		try (Connection c = ConnectionPool.getConnection()) {
			ObjectReader or = objectMapper.readerFor(Payload.class);
			Payload p = or.readValue(message);
			LinkedHashMap<String, Object> data = (LinkedHashMap<String, Object>) p.data;

			PreparedStatement st = c.prepareStatement(INSERT_SCRIPT);
			st.setLong(1, System.currentTimeMillis());
			st.setLong(2, (long) data.get("E"));
			st.setString(3, (String) data.get("s"));
			st.setString(4, (String) data.get("c"));
			st.setString(5, (String) data.get("o"));
			st.setString(6, (String) data.get("v"));
			st.execute();
      logger.info("INSERTED");

		} catch (JsonProcessingException e) {
			logger.error("Error parsing ws message, exception: {}", e.getMessage());
		} catch (SQLException e) {
			logger.error("Error inserting new mini_ticker, exception: {}", e.getMessage());
		}
	}

	@OnWebSocketConnect
	public void onConnect(Session session) {
		logger.info("Connected to the websocket.");
		this.session = session;
		latch.countDown();
	}

	public void sendMessage(String str) {
		try {
			session.getRemote().sendString(str);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public CountDownLatch getLatch() {
		return latch;
	}

	static class Payload {
		public String stream;
		public Object data;
	}

	private static final String INSERT_SCRIPT = "INSERT INTO mini_ticker(current_time_millis, epoch_time, pair, close, open, volume)\n" +
			"VALUES (?, ?, ?, ?, ?, ?);";

}
```
In the Socket.java class, we implement a classic WebSocket client. Every time
we get a web socket message, we call **onText** method and inside this method
we get a new connection from the connection pool and insert a new row. We use
a inner class called **Payload** to deserialize the string websocket message.

### Client.java
```java
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.io.Serializable;
import java.net.URI;
import java.util.List;

public class Client {

	private static ObjectMapper objectMapper = new ObjectMapper();
	private static WebSocketClient client = new WebSocketClient();
	static {
		try {
			Socket socket = new Socket();
			String dest = "wss://stream.binance.com:9443/stream";
			URI uri = new URI(dest);
			client.start();
			ClientUpgradeRequest request = new ClientUpgradeRequest();
			client.connect(socket, uri, request);
			socket.getLatch().await();

			// Sending Subscription message
			ObjectWriter ow = objectMapper.writer().withDefaultPrettyPrinter();
			ClientMessage msg = new ClientMessage(
					"SUBSCRIBE",
					List.of("btcusdt@miniTicker"),
					1
			);
			socket.sendMessage(ow.writeValueAsString(msg));

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	static class ClientMessage implements Serializable {
		public String method;

		public List<String> params;

		public Integer id;

		public ClientMessage(String method, List<String> params, Integer id) {
			this.method = method;
			this.params = params;
			this.id = id;
		}
	}
}
```
In Client.java class, we establish the binance websocket connection and
subscribe to BTCUSDT mini ticker stream in a static code block. In order to
handle the message serialization, we used an inner class called
**ClientMessage**.

4) And finally let's implement our main method.

### CryptoAggrApplication.java
```java
import io.javalin.Javalin;

public class CryptoAggrApplication {

	public static void main(String[] args) {

		Javalin app = Javalin.create();

		app.events(
				event -> {
					event.serverStarting(
							() -> {
								Class.forName("com.itwasneo.cryptoaggr.utils.ConnectionPool");
								Class.forName("com.itwasneo.cryptoaggr.utils.Client");
							}
					);
				}
		);

		app.start(7070);

	}
}
```
With javalin you can start a simple web server using a single line of code.
```java
Javalin app = Javalin.create().start(7070);
```
 **app.events** allows us to execute certain processes at specific lifecycle
 hooks. Here we initialize our ConnectionPool and websocket Client while the
 server is starting.

 At this point we are not communicating with the server, but we will. If you
 run the program, you should see an output like the following:

 ```
 [main] INFO org.eclipse.jetty.util.log - Logging initialized @238ms to org.eclipse.jetty.util.log.Slf4jLog
[main] INFO com.zaxxer.hikari.HikariDataSource - HikariPool-1 - Starting...
[main] INFO com.zaxxer.hikari.pool.HikariPool - HikariPool-1 - Added connection org.postgresql.jdbc.PgConnection@28f2a10f
[main] INFO com.zaxxer.hikari.HikariDataSource - HikariPool-1 - Start completed.
[WebSocketClient@417557780-25] INFO com.itwasneo.cryptoaggr.utils.Socket - Connected to the websocket.
[main] INFO io.javalin.Javalin - Starting Javalin ...
[main] INFO io.javalin.Javalin - You are running Javalin 4.6.4 (released July 8, 2022).
[main] INFO io.javalin.Javalin - Listening on http://localhost:7070/
[main] INFO io.javalin.Javalin - Javalin started in 2486ms \o/
[WebSocketClient@417557780-23] ERROR com.itwasneo.cryptoaggr.utils.Socket - Error parsing ws message, exception: Unrecognized field "result" (class com.itwasneo.cryptoaggr.utils.Socket$Payload), not marked as ignorable (2 known properties: "stream", "data"])
 at [Source: (String)"{"result":null,"id":1}"; line: 1, column: 15] (through reference chain: com.itwasneo.cryptoaggr.utils.Socket$Payload["result"])
[WebSocketClient@417557780-25] INFO com.itwasneo.cryptoaggr.utils.Socket - INSERTED
[WebSocketClient@417557780-23] INFO com.itwasneo.cryptoaggr.utils.Socket - INSERTED
[WebSocketClient@417557780-25] INFO com.itwasneo.cryptoaggr.utils.Socket - INSERTED
[WebSocketClient@417557780-23] INFO com.itwasneo.cryptoaggr.utils.Socket - INSERTED
[WebSocketClient@417557780-25] INFO com.itwasneo.cryptoaggr.utils.Socket - INSERTED
[WebSocketClient@417557780-23] INFO com.itwasneo.cryptoaggr.utils.Socket - INSERTED
[WebSocketClient@417557780-25] INFO com.itwasneo.cryptoaggr.utils.Socket - INSERTED
```
(After we sent our subscription message to the websocket, the first message we
receive is {"result":null,"id":1}, which doesn't match up with our Payload
class fields, so we can simply ignore the ERROR we are getting here.)

Fantastic!!! Since we populate our database automatically with binance data,
let's start with the second "microservice".

## Creating a javalin application that listens a kafka topic and uses an in-memory H2 database

1) Create a new Java application called crypto-connect which uses Maven build system.

2) Add our dependencies.

```xml
<repositories>
    <repository>
        <id>confluent</id>
        <name>confluent-repo</name>
        <url>https://packages.confluent.io/maven/</url>
    </repository>
</repositories>

<properties>
    <javalin-version>4.6.4</javalin-version>
    <h2-version>2.1.214</h2-version>
    <HikariCP-version>5.0.1</HikariCP-version>
    <kafka-clients-version>3.2.1</kafka-clients-version>
    <kafka-avro-serializer-version>5.3.0</kafka-avro-serializer-version>
    <jackson-databind-version>2.13.3</jackson-databind-version>
    <slf4j-simple-version>1.7.36</slf4j-simple-version>
</properties>

<dependencies>
    <dependency>
        <groupId>io.javalin</groupId>
        <artifactId>javalin</artifactId>
        <version>${javalin-version}</version>
    </dependency>

    <dependency>
        <groupId>com.h2database</groupId>
        <artifactId>h2</artifactId>
        <version>${h2-version}</version>
        <scope>runtime</scope>
    </dependency>

    <dependency>
        <groupId>com.zaxxer</groupId>
        <artifactId>HikariCP</artifactId>
        <version>${HikariCP-version}</version>
    </dependency>

    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>${kafka-clients-version}</version>
    </dependency>

    <dependency>
        <groupId>io.confluent</groupId>
        <artifactId>kafka-avro-serializer</artifactId>
        <version>${kafka-avro-serializer-version}</version>
    </dependency>

    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
        <version>${jackson-databind-version}</version>
    </dependency>

    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-simple</artifactId>
        <version>${slf4j-simple-version}</version>
    </dependency>

</dependencies>
```
In order to obtain **kafka-avro-serializer**, we need to add an external maven
repository belonging **confluent inc**.

3) For this "micro-service" we will need 2 utility class: ConnectionPool and Kafka

### ConnectionPool.java
```java
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionPool {

	private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

	private static HikariDataSource ds;
	static {
		try(InputStream input = Files.newInputStream(Paths.get("src/main/resources/datasource.properties"))) {
			// Configure Hikari DataSource
			Properties prop = new Properties();
			prop.load(input);
			HikariConfig config = new HikariConfig(prop);
			ds = new HikariDataSource(config);

			populateH2();
		} catch (IOException e) {
			throw new RuntimeException();
		}
	}

	public static Connection getConnection() throws SQLException {
		return ds.getConnection();
	}
	private static void populateH2() {
		try (Connection c = getConnection();
			 PreparedStatement ps = c.prepareStatement("CREATE TABLE mini_ticker ( " +
					 "id BIGINT PRIMARY KEY, " +
					 "current_time_millis BIGINT, " +
					 "epoch_time BIGINT, " +
					 "pair VARCHAR(10), " +
					 "close VARCHAR(20), " +
					 "open VARCHAR(20), " +
					 "volume VARCHAR(20));");) {
			ps.execute();
		} catch (Exception e) {
			logger.error("Error populating H2, exception: {}", e.getMessage());
		}
	}

	private ConnectionPool() {}
}
```

### datasource.properties file for H2 in-memory database with Hikari Connection Pool
```properties
jdbcUrl=jdbc:h2:mem:pg_dev_replica
username=itwasneo
password=
connectionTimeout=5000
maxLifetime=30000
maximumPoolSize=10
```
Unlike our first ConnectionPool.java class inside the crypto-aggr application,
this time we will create our mini_ticker table inside our in-memory database
each time we initialize our ConnectionPool. (Due to the fact that everytime we
stop the application we discard the **in-memory** database completely.)

### Kafka.java
```java
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class Kafka {

	private static final Logger logger = LoggerFactory.getLogger(Kafka.class);

	private static Consumer<String, GenericRecord> consumer;
	// CONFIGURATION
	static {
		try(InputStream input = Files.newInputStream(Paths.get("src/main/resources/kafka.properties"))) {

			// Collect properties from kafka.properties file.
			Properties p = new Properties();
			p.load(input);

			// Fill a new Properties object for Kafka consumer constructor and initialize the consumer.
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					p.getProperty("bootstrapAddress"));
			props.put(ConsumerConfig.GROUP_ID_CONFIG,
					p.getProperty("groupID") + "-" + UUID.randomUUID());
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					KafkaAvroDeserializer.class);
			props.put(
					"schema.registry.url",
					p.getProperty("schemaRegistryURL"));
			props.put(
					ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
					"earliest"
			);
			consumer = new KafkaConsumer<>(props);

			// Subscribe to topics inside the property file.
			consumer.subscribe(Collections.singletonList(p.getProperty("topic")));

			startConsumer();

		} catch (Exception e) {
			logger.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	public static void startConsumer() {
		try {
			while (true) {
				ConsumerRecords<String, GenericRecord> consumerRecords = consumer.poll(Duration.ofMillis(100));

				consumerRecords.forEach(r -> {
					try (Connection c = ConnectionPool.getConnection()) {
						GenericRecord message = r.value();

						if (Boolean.parseBoolean(message.get("__deleted").toString())) {
							long id = (long)message.get("ID");
							PreparedStatement pst = c.prepareStatement(DELETE_SCRIPT);
							pst.setLong(1, id);
							pst.executeQuery();
						} else {
							PreparedStatement pst = c.prepareStatement(MERGE_SCRIPT);
							pst.setInt(1, (int) message.get("id"));
							pst.setLong(2, System.currentTimeMillis());
							pst.setLong(3, (long) message.get("epoch_time"));
							pst.setString(4, message.get("pair").toString());
							pst.setString(5, message.get("close").toString());
							pst.setString(6, message.get("open").toString());
							pst.setString(7, message.get("volume").toString());
							int dmlExecuted = pst.executeUpdate();
							logger.info("INSERTED {} ROW", dmlExecuted);
						}

					} catch(Exception e) {
						logger.error("Error handling Kafka message, exception: {}", e.getMessage());
					}
				});
				consumer.commitAsync();
			}
		} catch (Exception e) {
			logger.error("Error listening Kafka, exception: {}", e.getMessage());
		} finally {
			consumer.close();
		}
	}

	private static final String MERGE_SCRIPT =
			"MERGE INTO mini_ticker " +
					"(id, current_time_millis, epoch_time, pair, close, open, volume) " +
					"KEY (id) VALUES (?, ?, ?, ?, ?, ?, ?);";

	private static final String DELETE_SCRIPT =
			"DELETE FROM mini_ticker where id = ?;";

	private Kafka() {}
}
```
### kafka.properties
```properties
topic=pg_dev.public.mini_ticker
groupID=cryptoksl
bootstrapAddress=localhost:9092
schemaRegistryURL=http://localhost:8081
```
Inside Kafka.java class, we implement a basic Kafka consumer. In a static code
block, we configure our Kafka consumer using **kafka.properties** file,
subscribe to our **pg_dev.public.mini_ticker** topic and start polling. Each
time we consume a **record** or **message** from the topic, we either merge a
new row into our in-memory database, or delete a row from it depending on the
**__deleted** flag of each record.

4) And finally we implement our **main** method.

### CryptoConnectApplication.java
```java
import io.javalin.Javalin;

public class CryptoConnectApplication {

	public static void main(String[] args) {
		Javalin app = Javalin.create();

		app.events(
			event -> {
				event.serverStarting(
						() -> {
							Class.forName("com.itwasneo.cryptoconnect.utils.ConnectionPool");
							Class.forName("com.itwasneo.cryptoconnect.utils.Kafka");
						}
				);
			}
		);

		app.start(7071);
	}
}
```
In order to test whether this service working correctly or not, we need our
utilities up and running. After running our **docker-compose-crypto-utils.yml**
file and connecting kafka-connect to our postgres database, we can run our
second "micro-service".

At the end of the application log, you should see something like the following:
```
.
.
.
[main] INFO com.itwasneo.cryptoconnect.utils.Kafka - INSERTED 1 ROW
[main] INFO com.itwasneo.cryptoconnect.utils.Kafka - INSERTED 1 ROW
[main] INFO com.itwasneo.cryptoconnect.utils.Kafka - INSERTED 1 ROW
[main] INFO com.itwasneo.cryptoconnect.utils.Kafka - INSERTED 1 ROW
[main] INFO com.itwasneo.cryptoconnect.utils.Kafka - INSERTED 1 ROW
[main] INFO com.itwasneo.cryptoconnect.utils.Kafka - INSERTED 1 ROW
[main] INFO com.itwasneo.cryptoconnect.utils.Kafka - INSERTED 1 ROW
[main] INFO com.itwasneo.cryptoconnect.utils.Kafka - INSERTED 1 ROW
[main] INFO com.itwasneo.cryptoconnect.utils.Kafka - INSERTED 1 ROW
[main] INFO com.itwasneo.cryptoconnect.utils.Kafka - INSERTED 1 ROW
```
As you can see, in our **pg_dev.public.mini_ticker** topic created by
**kafka-connect** we have an **immutable source of truth** that we can use to
replicate our entire table from scratch.

If you run both services at the same time, you will see that once a new row
inserted into our postgres table by the crypto-aggr app, a new row is created
by crypto-connect app inside its own database as well.
