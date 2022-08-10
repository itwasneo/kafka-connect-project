package com.itwasneo.cryptoconnect;

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
