package DTO;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class WeatherDTO {

    int humidity;

    int temperature;

    int windSpeed;
}
