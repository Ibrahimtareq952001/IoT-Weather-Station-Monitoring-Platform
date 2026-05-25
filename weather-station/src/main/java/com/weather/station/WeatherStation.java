package com.weather.station;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.station.model.WeatherData;
import com.weather.station.model.WeatherReading;
import com.weather.station.producer.WeatherKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates a single IoT weather station.
 *
 * Usage:
 *   java -jar weather-station.jar <station_id>
 *
 * Behaviour is controlled by environment variables:
 *   KAFKA_BOOTSTRAP_SERVERS  — if set, produces to Kafka; otherwise prints to stdout.
 *                              Example: localhost:9094  (host dev)
 *                                       kafka:9092      (inside Docker / K8s)
 */
public class WeatherStation {

    private static final Logger log = LoggerFactory.getLogger(WeatherStation.class);

    // Battery distribution: index 0-2 → low (30%), 3-6 → medium (40%), 7-9 → high (30%)
    // Uses a pool of 10 strings so random.nextInt(10) gives exact percentages,
    // not approximate ones like you'd get from if/else on a double.
    private static final String[] BATTERY_POOL = {
        "low", "low", "low",
        "medium", "medium", "medium", "medium",
        "high", "high", "high"
    };

    // Every event (sent OR dropped) consumes one s_no.
    // This creates gaps in the stream that the Central Station uses to count drops.
    // Example: s_no 1,2,[3 dropped],4 → Central Station sees gap at 3 = 1 drop detected.
    private static final double DROP_RATE = 0.10;

    private final long stationId;
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    private final Random random = new Random();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WeatherStation(long stationId) {
        this.stationId = stationId;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java -jar weather-station.jar <station_id>");
            System.exit(1);
        }

        long stationId = Long.parseLong(args[0]);
        new WeatherStation(stationId).run();
    }

    private void run() throws Exception {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");

        if (bootstrapServers != null && !bootstrapServers.isBlank()) {
            log.info("Station {} → Kafka mode ({})", stationId, bootstrapServers);
            // try-with-resources ensures producer.flush()+close() on shutdown
            try (WeatherKafkaProducer producer = new WeatherKafkaProducer(bootstrapServers)) {
                runLoop(producer);
            }
        } else {
            log.info("Station {} → stdout mode (set KAFKA_BOOTSTRAP_SERVERS to enable Kafka)", stationId);
            runLoop(null);
        }
    }

    private void runLoop(WeatherKafkaProducer producer) throws InterruptedException {
        while (true) {
            if (random.nextDouble() < DROP_RATE) {
                long dropped = sequenceNumber.incrementAndGet();
                log.debug("Station {} dropped s_no={}", stationId, dropped);
                Thread.sleep(1000);
                continue;
            }

            WeatherReading reading = generateReading();

            if (producer != null) {
                producer.send(reading);
            } else {
                System.out.println(serialize(reading));
            }

            Thread.sleep(1000);
        }
    }

    private WeatherReading generateReading() {
        return new WeatherReading(
            stationId,
            sequenceNumber.incrementAndGet(),
            BATTERY_POOL[random.nextInt(BATTERY_POOL.length)],
            System.currentTimeMillis() / 1000L,
            new WeatherData(
                random.nextInt(101),        // humidity:     0–100 %
                random.nextInt(81) + 20,    // temperature: 20–100 °F (matches spec example of 100)
                random.nextInt(101)         // wind speed:   0–100 km/h
            )
        );
    }

    private String serialize(WeatherReading reading) {
        try {
            return objectMapper.writeValueAsString(reading);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
