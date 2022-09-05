package com.itwasneo.cryptoaggr.utils;

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

			Properties prop = new Properties();
			prop.load(input);
			HikariConfig config = new HikariConfig(prop);
			ds = new HikariDataSource(config);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static Connection getConnection() throws SQLException {
		return ds.getConnection();
	}

	private ConnectionPool() {}
}
