package com.itwasneo.cryptoaggr.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.itwasneo.cryptoaggr.CryptoAggrApplication;
import io.javalin.http.sse.SseClient;
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
import java.util.Iterator;
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
		try (Connection c = ConnectionPool.getConnection();
			 PreparedStatement st = c.prepareStatement(INSERT_SCRIPT)) {

			ObjectReader or = objectMapper.readerFor(Payload.class);
			Payload p = or.readValue(message);
			LinkedHashMap<String, Object> data = (LinkedHashMap<String, Object>) p.data;

			st.setLong(1, System.currentTimeMillis());
			st.setLong(2, (long) data.get("E"));
			st.setString(3, (String) data.get("s"));
			st.setString(4, (String) data.get("c"));
			st.setString(5, (String) data.get("o"));
			st.setString(6, (String) data.get("v"));
			st.execute();

			logger.info("INSERTED");

			for (SseClient client : CryptoAggrApplication.CLIENTS) {
				client.sendEvent(new Event(System.currentTimeMillis(), (String)data.get("s"), (String)data.get("c")));
			}

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
