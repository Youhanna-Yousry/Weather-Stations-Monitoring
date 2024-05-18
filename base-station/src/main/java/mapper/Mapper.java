package mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import dto.CompactStationMsgDTO;
import dto.StationStatusMsgDTO;

import java.io.IOException;

@org.mapstruct.Mapper
public interface Mapper {

    ObjectMapper mapper = new ObjectMapper();

    default CompactStationMsgDTO deserializeCompactStationMsg(byte[] byteArray) throws IOException {
        return mapper.readValue(byteArray, CompactStationMsgDTO.class);
    }

    StationStatusMsgDTO compactStationMsgToStationStatusMsg(CompactStationMsgDTO compactStationMsgDTO,
                                                            long stationId,
                                                            long statusTimestamp);

    default byte[] serializeStationStatusMsg(StationStatusMsgDTO stationStatusMsgDTO) throws IOException {
        return mapper.writeValueAsBytes(stationStatusMsgDTO);
    }

    default StationStatusMsgDTO deserializeStationStatusMsg(byte[] byteArray) throws IOException {
        return mapper.readValue(byteArray, StationStatusMsgDTO.class);
    }
}
