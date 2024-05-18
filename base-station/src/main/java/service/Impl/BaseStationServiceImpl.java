package service.Impl;

import parquetDao.ArchiveToParquet;
import parquetDao.DAO;
import dto.StationStatusMsgDTO;
import service.BaseStationService;

public class BaseStationServiceImpl implements BaseStationService {

    @Override
    public void serveMessage(StationStatusMsgDTO stationStatusMsgDTO) {
        // save the message to bitcask

        // save the message to parquet files
        DAO archive = new ArchiveToParquet();
        archive.writeToParquet(stationStatusMsgDTO);
    }
}
