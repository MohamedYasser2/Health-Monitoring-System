package com.healthmonitor.healthmonitorbackend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.sql.SQLException;

@SpringBootApplication
public class HealthMonitorBackendApplication {

	public static void main(String[] args) {
		DuckDBManager duckDBManager = new DuckDBManager();
		try {
			duckDBManager.initializeConnection();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		SpringApplication.run(HealthMonitorBackendApplication.class, args);
	}

}
