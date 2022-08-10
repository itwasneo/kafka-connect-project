package com.itwasneo.cryptoaggr;

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
