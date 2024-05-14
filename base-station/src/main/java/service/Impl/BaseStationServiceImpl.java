package service.Impl;

import dao.ArchiveToParquet;
import dto.StationStatusMsgDTO;
import org.checkerframework.checker.units.qual.A;
import service.BaseStationService;

public class BaseStationServiceImpl implements BaseStationService {

    @Override
    public void serveMessage(StationStatusMsgDTO stationStatusMsgDTO) {
        // TODO save the message to bitcask
        // TODO save the message to parquet files
        ArchiveToParquet archive = new ArchiveToParquet();
        archive.writeToParquet(stationStatusMsgDTO);
    }
}
