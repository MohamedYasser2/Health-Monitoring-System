package com.healthmonitor.healthmonitorbackend;

import com.healthmonitor.healthmonitorbackend.spark.SparkThread;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;



@SpringBootApplication
public class HealthMonitorBackendApplication {

	public static void main(String[] args) {
		SpringApplication.run(HealthMonitorBackendApplication.class, args);
	}

}
