package com.akash.temperature;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Properties;

@EnableKafkaStreams
@Service
public class KafkaStreamProcessor {

    private final StreamsBuilder streamsBuilder;
    private KafkaStreams kafkaStreams;

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    public KafkaStreamProcessor(StreamsBuilder streamsBuilder) {
        this.streamsBuilder = streamsBuilder;
    }

    @PostConstruct
    public void startStream() {
        try {
            // Create the stream from the "temperature_readings" topic
            KStream<String, String> temperatureStream = streamsBuilder.stream("temperature_readings");

            temperatureStream
                    .peek((key, value) -> System.out.println("Received: " + value)) // Log received values
                    .filter((key, value) -> {
                        try {
                            String[] parts = value.split(",");
                            double temperature = Double.parseDouble(parts[1]);
                            boolean isAbove20 = temperature > 20;
                            System.out.println("Processing: " + value + " | Temperature: " + temperature + " | Pass filter: " + isAbove20);
                            return isAbove20; // Only pass temperatures above 20°C
                        } catch (Exception e) {
                            System.err.println("Error processing value: " + value + " | Error: " + e.getMessage());
                            return false; // Skip messages that can't be parsed correctly
                        }
                    })
                    .to("filtered_temperature_readings", Produced.with(Serdes.String(), Serdes.String()));

            // Build the topology
            Topology topology = streamsBuilder.build();
            kafkaStreams = new KafkaStreams(topology, getStreamsConfig());

            // Attach shutdown hook for graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

            // Start the Kafka Streams application
            kafkaStreams.start();
            System.out.println("Stream processing started...");
        } catch (Exception e) {
            System.err.println("Error starting Kafka Streams: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private Properties getStreamsConfig() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "temperature-stream");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }

    @PreDestroy
    public void closeStream() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
            System.out.println("Stream processing stopped...");
        }
    }
}



