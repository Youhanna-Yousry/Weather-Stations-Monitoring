package DTO;

import lombok.*;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class StationStatusMessageDTO {

    long stationId;

    long sequenceNumber;

    String batteryStatus;

    long statusTimestamp;

    WeatherDTO weather;
}
