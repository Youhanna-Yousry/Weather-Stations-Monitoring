package service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dto.CompactStationMsgDTO;
import dto.WeatherDTO;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.json.JSONException;
import service.WeatherStation;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class WeatherStationImpl implements WeatherStation {
    private static final String TOPIC = "weather-station-topic";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private final AtomicInteger sequenceNumber = new AtomicInteger(0);
    private final long stationID ;
    private final String latitude;
    private final String longitude;
    private List<WeatherDTO> hourlyWeather;
    private int currentIndex = 0;
    private static final Random RANDOM = new Random();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(WeatherStationImpl.class);
    public WeatherStationImpl(String latitude, String longitude){
        this.stationID = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;
        this.latitude = latitude;
        this.longitude = longitude;
    }
    public void produceMessage() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("key.serializer", LongSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());

        try (Producer<Long, byte[]> producer = new KafkaProducer<>(props)) {
            while (true) {
                if (hourlyWeather == null || currentIndex >= hourlyWeather.size()) {
                    hourlyWeather = new OpenMeteoImpl(latitude, longitude).fetchHourlyWeatherData();
                    currentIndex = 0;
                }

                if (currentIndex < hourlyWeather.size()) {
                    if (RANDOM.nextDouble() > 0.1) {
                        WeatherDTO weather = hourlyWeather.get(currentIndex++);
                        String batteryStatus = getBatteryStatus();
                        CompactStationMsgDTO compactMessage = new CompactStationMsgDTO(sequenceNumber.incrementAndGet(), batteryStatus, weather);
                        byte[] messageBytes = OBJECT_MAPPER.writeValueAsBytes(compactMessage);
                        producer.send(new ProducerRecord<>(TOPIC, stationID, messageBytes));
                        producer.flush();
                        logger.info("Sent: {}", compactMessage);
                        //System.out.println("Sent: " + compactMessage);
                    }
                    else{
                        currentIndex++;
                        sequenceNumber.incrementAndGet();
                        System.out.println("message dropped");
                        logger.debug("Message dropped intentionally.");
                        continue;
                    }
                }

                Thread.sleep(1000);
            }
        } catch (JsonProcessingException | InterruptedException | JSONException e) {
            logger.error("Interrupted!", e);
            throw new RuntimeException(e);
        }
    }

    private String getBatteryStatus() {
        int rand = RANDOM.nextInt(10);
        if (rand < 3) {
            return "low";
        } else if (rand < 7) {
            return "medium";
        } else {
            return "high";
        }
    }
    public static double generateLatitude() {
        // Latitude can range from -90.0 to 90.0
        return RANDOM.nextDouble() * 180.0 - 90.0;  // Scale to [-90, 90]
    }

    public static double generateLongitude() {
        // Longitude can range from -180.0 to 180.0
        return RANDOM.nextDouble() * 360.0 - 180.0;  // Scale to [-180, 180]
    }

    public static void main(String[] args) {
        double latitude = generateLatitude();
        double longitude = generateLongitude();
        Thread producerThread = new Thread(() -> {
            WeatherStationImpl weatherStation = new WeatherStationImpl(String.valueOf(latitude) , String.valueOf(longitude));
            weatherStation.produceMessage();
        });
        producerThread.start();
        try {
            producerThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
