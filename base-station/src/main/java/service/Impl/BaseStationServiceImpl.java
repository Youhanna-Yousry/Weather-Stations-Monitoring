package service.Impl;

import dto.StationStatusMsgDTO;
import service.BaseStationService;

public class BaseStationServiceImpl implements BaseStationService {

    @Override
    public void serveMessage(StationStatusMsgDTO stationStatusMsgDTO) {
        // TODO save the message to bitcask
        // TODO save the message to parquet files
    }
}
