package dto;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class CompactStationMsgDTO {

    long sequenceNumber;

    String batteryStatus;

    WeatherDTO weather;
}
