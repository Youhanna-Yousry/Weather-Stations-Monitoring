import service.impl.WeatherStationImpl;

import static utils.helpers.generateLatitude;
import static utils.helpers.generateLongitude;

public class Main {
    public static void main(String[] args) {
        double latitude = generateLatitude();
        double longitude = generateLongitude();

            WeatherStationImpl weatherStation = new WeatherStationImpl(String.valueOf(latitude), String.valueOf(longitude));
            weatherStation.produceMessage();

    }
}