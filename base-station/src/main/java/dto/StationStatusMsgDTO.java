package dto;

import lombok.*;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class StationStatusMsgDTO {

    long stationId;

    long sequenceNumber;

    String batteryStatus;

    long statusTimestamp;

    WeatherDTO weather;
}
