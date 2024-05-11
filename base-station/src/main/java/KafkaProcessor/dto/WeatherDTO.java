package KafkaProcessor.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class WeatherDTO {

    int humidity;

    int temperature;

    @JsonProperty("wind_speed")
    int windSpeed;
}