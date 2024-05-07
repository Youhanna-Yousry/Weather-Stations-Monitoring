package services.Impl;

import DTO.StationStatusMessageDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import services.BaseStation;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class BaseStationImpl implements BaseStation {

    private static final String TOPIC = "weather-station-topic";
    private static final String GROUP_ID = "weather-station-group";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String ENABLE_AUTO_COMMIT = "true";
    private static final String AUTO_COMMIT_INTERVAL = "1000";
    private static final String AUTO_OFFSET_RESET = "earliest";

    private final ObjectMapper mapper = new ObjectMapper();

//    private void produceMessage() {
//        Properties props = new Properties();
//        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
//        props.put("key.serializer", LongSerializer.class.getName());
//        props.put("value.serializer", ByteArraySerializer.class.getName());
//
//        ObjectMapper mapper = new ObjectMapper();
//
//
//        // create a producer that send the message(key = message id, value = message content) to the topic
//        try (Producer<Long, byte[]> producer = new KafkaProducer<>(props)) {
//            for (int i = 0; i < 5; i++) {
//                StationStatusMessageDTO dto = new StationStatusMessageDTO(1, 1, "good", 1, new WeatherDTO(1, 1, 1));
//                String message = mapper.writeValueAsString(dto);
//                producer.send(new ProducerRecord<>(TOPIC, 1000L, message.getBytes()));
//                producer.flush();
//            }
//        } catch (JsonProcessingException e) {
//            throw new RuntimeException(e);
//        }
//
//    }

    private static Properties getProperties() {
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
                    StationStatusMessageDTO message = mapper.readValue(record.value(), StationStatusMessageDTO.class);
                    // TODO archive the message in parquet files
                    // TODO archive the message in bitcask
                }
            }
        } catch (IOException e) {
            System.exit(1);
        }
    }


//    public static void main(String[] args) {
//        Thread producerThread = new Thread(() -> {
//            BaseStationImpl baseStation = new BaseStationImpl();
//            baseStation.produceMessage();
//        });
//
//        Thread consumerThread = new Thread(() -> {
//            BaseStationImpl baseStation = new BaseStationImpl();
//            baseStation.consumeMessage();
//        });
//
//        producerThread.start();
//        consumerThread.start();
//
//        try {
//            producerThread.join();
//            consumerThread.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
}
