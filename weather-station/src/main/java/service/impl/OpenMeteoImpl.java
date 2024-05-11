package service.impl;

import dto.WeatherDTO;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class OpenMeteoImpl {

    private final String url;

    public OpenMeteoImpl(String latitude, String longitude) {
        this.url = "https://api.open-meteo.com/v1/forecast?latitude=" + latitude +
                "&longitude=" + longitude + "&hourly=temperature_2m,relativehumidity_2m,windspeed_10m" +
                "&current_weather=true&temperature_unit=fahrenheit&timeformat=unixtime" +
                "&forecast_days=1&timezone=Africa%2FCairo";
    }

    private String fetchDataFromOpenMeteo() {
        String responseBody = "";
        try {
            URL url = new URL(this.url);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");

            Scanner scanner = new Scanner(conn.getInputStream());
            while (scanner.hasNext()) {
                responseBody += scanner.nextLine();
            }
            scanner.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return responseBody;
    }

    public List<WeatherDTO> fetchHourlyWeatherData() throws JSONException {
        String responseBody = fetchDataFromOpenMeteo();
        JSONObject jsonObject = new JSONObject(responseBody);
        JSONObject hourly = jsonObject.getJSONObject("hourly");
        JSONArray temperatures = hourly.getJSONArray("temperature_2m");
        JSONArray humidities = hourly.getJSONArray("relativehumidity_2m");
        JSONArray windSpeeds = hourly.getJSONArray("windspeed_10m");

        List<WeatherDTO> weatherData = new ArrayList<>();
        for (int i = 0; i < temperatures.length(); i++) {
            int temperature = temperatures.getInt(i);
            int humidity = humidities.getInt(i);
            int windSpeed = windSpeeds.getInt(i);
            weatherData.add(new WeatherDTO(humidity, temperature, windSpeed));
        }

        return weatherData;
    }

    //public static void main(String[] ags) throws JSONException {
      //  List<WeatherDTO> weather = new OpenMeteoImpl("30.0444", "31.2357").fetchHourlyWeatherData();
       // System.out.println(weather);
    //}

}
