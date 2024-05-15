package consumer.Impl;

import com.google.inject.name.Named;
import consumer.BaseStationConsumer;
import dto.CompactStationMsgDTO;
import dto.StationStatusMsgDTO;
import jakarta.inject.Inject;
import mapper.Mapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.slf4j.Logger;
import service.BaseStationService;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BaseStationConsumerImpl implements BaseStationConsumer {

    private static final String TOPIC = "weather-station-topic";
    private static final String GROUP_ID = "weather-station-group";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String ENABLE_AUTO_COMMIT = "true";
    private static final String AUTO_COMMIT_INTERVAL = "1000";
    private static final String AUTO_OFFSET_RESET = "earliest";

    @Inject
    @Named("ConsumerLogger")
    private Logger logger;
    @Inject
    private Mapper mapper;
    @Inject
    private BaseStationService baseStationService;

    private Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);
        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, AUTO_COMMIT_INTERVAL);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
        return props;
    }

    @Override
    public void consumeMessage() {
        Properties props = getProperties();

        try (KafkaConsumer<Long, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            while (true) {
                ConsumerRecords<Long, byte[]> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<Long, byte[]> record : records) {
                    CompactStationMsgDTO compactMessage = mapper.deserializeCompactStationMsg(record.value());
                    StationStatusMsgDTO message = mapper.compactStationMsgToStationStatusMsg(
                            compactMessage,
                            record.key(),
                            record.timestamp());
                    baseStationService.serveMessage(message);
                }
            }
        } catch (IOException e) {
            logger.error("Failed to consume messages", e);
        }

    }
}
