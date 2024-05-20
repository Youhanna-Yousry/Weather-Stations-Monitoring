package dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class CompactStationMsgDTO {

    @JsonProperty("s_no")
    long sequenceNumber;

    @JsonProperty("battery_status")
    String batteryStatus;

    WeatherDTO weather;
}
