package com.itwasneo.cryptoconnect;

import com.itwasneo.cryptoconnect.utils.Kafka;
import io.javalin.Javalin;
import io.javalin.http.sse.SseClient;

import java.util.concurrent.ConcurrentLinkedQueue;

public class CryptoConnectApplication {

	public static final ConcurrentLinkedQueue<SseClient> CLIENTS = new ConcurrentLinkedQueue<>();

	public static void main(String[] args) {
		Javalin app = Javalin.create(conf -> {
			conf.enableCorsForAllOrigins();
		}).events(
			event -> {
				event.serverStarting(
						() -> {
							Class.forName("com.itwasneo.cryptoconnect.utils.ConnectionPool");
							Class.forName("com.itwasneo.cryptoconnect.utils.Kafka");
						}
				);
				event.serverStarted(
						() -> {
							Kafka.startConsumer();
						}
				);
			}
		);

		app.sse("/sse", client -> {
			CLIENTS.add(client);
			client.onClose(() -> CLIENTS.remove(client));
		});

		app.start(7071);

	}
}
