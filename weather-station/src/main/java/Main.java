import service.impl.WeatherStationImpl;

import static utils.helpers.generateLatitude;
import static utils.helpers.generateLongitude;

public class Main {
    public static void main(String[] args) {
        double latitude = generateLatitude();
        double longitude = generateLongitude();
        Thread producerThread = new Thread(() -> {
            WeatherStationImpl weatherStation = new WeatherStationImpl(String.valueOf(latitude), String.valueOf(longitude));
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