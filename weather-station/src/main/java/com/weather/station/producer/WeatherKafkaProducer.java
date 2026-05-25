package com.weather.station.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.weather.station.model.WeatherReading;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Wraps KafkaProducer and handles serialization + error logging.
 *
 * Key design choices explained:
 *
 * MESSAGE KEY = station_id
 *   Kafka routes all records with the same key to the same partition.
 *   This guarantees that messages from Station 3 always land in the same
 *   partition, so a consumer reading that partition sees them in order.
 *   Without this, Station 3 messages could scatter across partitions and
 *   arrive out of order at the Central Station.
 *
 * acks=all
 *   The broker only acknowledges the send once ALL in-sync replicas have
 *   written the record. Strongest durability guarantee. In a single-broker
 *   dev setup this behaves identically to acks=1, but the config is correct
 *   for production from day one.
 *
 * linger.ms=5
 *   The producer waits up to 5ms before sending a batch, allowing multiple
 *   records to be grouped into one network request. Since each station sends
 *   1 msg/sec this has negligible effect now, but is correct practice.
 *
 * retries=3
 *   On transient network errors, retry up to 3 times before failing.
 */
public class WeatherKafkaProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WeatherKafkaProducer.class);

    public static final String TOPIC = "weather-readings";

    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WeatherKafkaProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG,              "all");
        props.setProperty(ProducerConfig.RETRIES_CONFIG,           "3");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG,         "5");
        // Idempotence: prevents duplicate delivery on retry after network timeout.
        // Kafka 3.x enables this automatically when acks=all, but explicit is safer.
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        this.producer = new KafkaProducer<>(props);
        log.info("Kafka producer connected to {}", bootstrapServers);
    }

    /**
     * Sends asynchronously. The callback logs any delivery failure without
     * crashing the station — a dropped delivery is treated the same as a
     * simulated network drop.
     */
    public void send(WeatherReading reading) {
        try {
            String key   = String.valueOf(reading.getStationId());
            String value = objectMapper.writeValueAsString(reading);

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Station {} failed to deliver s_no={}: {}",
                        reading.getStationId(), reading.getSequenceNumber(), exception.getMessage());
                } else {
                    log.debug("Delivered s_no={} → partition={} offset={}",
                        reading.getSequenceNumber(), metadata.partition(), metadata.offset());
                }
            });

        } catch (Exception e) {
            log.error("Serialization error for station {}: {}", reading.getStationId(), e.getMessage());
        }
    }

    @Override
    public void close() {
        // flush() blocks until all buffered records are sent before closing.
        // Without this, buffered records are discarded on shutdown.
        producer.flush();
        producer.close();
        log.info("Kafka producer closed");
    }
}
