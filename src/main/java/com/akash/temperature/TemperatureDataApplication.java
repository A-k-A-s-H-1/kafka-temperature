package com.akash.temperature;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling  // Enables scheduling for the TemperatureDataProducer to produce data at intervals
public class TemperatureDataApplication {

	public static void main(String[] args) {
		SpringApplication.run(TemperatureDataApplication.class, args);
	}
}

