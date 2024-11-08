package com.akash.temperature;



import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@EnableKafka
public class TemperatureDataConsumer {

    private final TemperatureDataRepository repository;

    public TemperatureDataConsumer(TemperatureDataRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(topics = "filtered_temperature_readings", groupId = "temperature-consumer-group")
    public void consume(String message) {
        try {
            String[] parts = message.split(",");
            if (parts.length == 2) {
                String city = parts[0].trim();
                double temperature = Double.parseDouble(parts[1].trim());

                // Save to H2 Database
                TemperatureData data = new TemperatureData(city, temperature);
                repository.save(data);

                System.out.println("Saved: " + message);
            } else {
                System.err.println("Invalid message format: " + message);
            }
        } catch (Exception e) {
            System.err.println("Error processing message: " + message);
            e.printStackTrace();
        }
    }

}

