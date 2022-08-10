package com.itwasneo.cryptoaggr.utils;

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
