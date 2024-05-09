package service;

import dto.StationStatusMsgDTO;

public interface BaseStationService {

    void serveMessage(StationStatusMsgDTO stationStatusMsgDTO);
}
