package com.itwasneo.cryptoconnect.utils;

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
		try(InputStream input = Files.newInputStream(Paths.get("config/datasource.properties"))) {
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
