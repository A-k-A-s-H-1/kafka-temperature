package com.akash.temperature;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Properties;
import java.util.Random;

@Service
public class TemperatureDataProducer {

    private final KafkaProducer<String, String> producer;
    private static final String TOPIC = "temperature_readings";
    private final Random random = new Random();

    public TemperatureDataProducer(@Value("${kafka.bootstrap-servers}") String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    @Scheduled(fixedDelay = 1000)  // Sends data every second
    public void produceData() {
        String city = "City" + (random.nextInt(5) + 1);
        double temperature = 15 + (random.nextDouble() * 20);
        String message = city + "," + String.format("%.2f", temperature);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, city, message);
        producer.send(record);
        System.out.println("Sent: " + message);
    }
}


