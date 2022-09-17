package com.itwasneo.cryptoaggr;

import io.javalin.Javalin;
import io.javalin.http.sse.SseClient;

import java.util.concurrent.ConcurrentLinkedQueue;

public class CryptoAggrApplication {

	public static final ConcurrentLinkedQueue<SseClient> CLIENTS = new ConcurrentLinkedQueue<>();

	public static void main(String[] args) {

		Javalin app = Javalin.create(conf -> {
			conf.enableCorsForAllOrigins();
		});

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

		app.sse("/sse", client -> {
			CLIENTS.add(client);
			client.onClose(() -> CLIENTS.remove(client));
		});

		app.start(7070);

	}

}
