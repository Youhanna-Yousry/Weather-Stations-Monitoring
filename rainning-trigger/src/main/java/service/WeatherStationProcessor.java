package service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Properties;

import dto.CompactStationMsgDTO;

import org.apache.kafka.streams.kstream.Produced;

public class WeatherStationProcessor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public void start() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-station-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, CompactStationMsgDTO> stream = builder.stream(
                "weather-station-topic",
                Consumed.with(Serdes.Long(), getCompactStationMsgDTOSerde())
        );
        KStream<Long, String> rainEvents = stream
                .filter((key, value) ->
                        value.getWeather().getHumidity() > 70)
                .mapValues(value -> "High humidity detected at Station with sequence: " + value.getSequenceNumber());

        rainEvents.to("rainy-topic", Produced.with(Serdes.Long(), Serdes.String()));

        rainEvents.foreach((key, value) -> System.out.println("stationID " + key + ", Data: " + value));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Attach shutdown hook to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Serde<CompactStationMsgDTO> getCompactStationMsgDTOSerde() {
        return Serdes.serdeFrom(new CompactStationMsgDTOSerializer(), new CompactStationMsgDTODeserializer());
    }

    public static class CompactStationMsgDTODeserializer implements Deserializer<CompactStationMsgDTO> {

        @Override
        public CompactStationMsgDTO deserialize(String topic, byte[] data) {
            try {
                return OBJECT_MAPPER.readValue(data, CompactStationMsgDTO.class);
            } catch (IOException e) {
                throw new RuntimeException("Error deserializing JSON message", e);
            }
        }


        @Override
        public void close() {
        }
    }

    public static class CompactStationMsgDTOSerializer implements Serializer<CompactStationMsgDTO> {


        @Override
        public byte[] serialize(String topic, CompactStationMsgDTO data) {
            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new RuntimeException("Error serializing to JSON", e);
            }
        }

        @Override
        public void close() {

        }
    }

}
