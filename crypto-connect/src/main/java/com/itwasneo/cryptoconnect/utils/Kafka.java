package com.itwasneo.cryptoconnect.utils;

import com.itwasneo.cryptoconnect.CryptoConnectApplication;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.javalin.http.sse.SseClient;
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
		try(InputStream input = Files.newInputStream(Paths.get("config/kafka.properties"))) {

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

							for (SseClient client : CryptoConnectApplication.CLIENTS) {
								client.sendEvent(new Event(System.currentTimeMillis(), message.get("pair").toString(), message.get("close").toString()));
							}
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
