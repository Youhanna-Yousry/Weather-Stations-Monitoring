package utils;

import java.util.Random;


public class helpers {
    private static final Random RANDOM = new Random();

    public static String getBatteryStatus() {
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
}
