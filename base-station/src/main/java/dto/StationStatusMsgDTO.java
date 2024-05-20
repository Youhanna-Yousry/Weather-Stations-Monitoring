package dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class StationStatusMsgDTO {

    @JsonProperty("station_id")
    long stationId;

    @JsonProperty("s_no")
    long sequenceNumber;

    @JsonProperty("battery_status")
    String batteryStatus;

    @JsonProperty("status_timestamp")
    long statusTimestamp;

    WeatherDTO weather;
}
