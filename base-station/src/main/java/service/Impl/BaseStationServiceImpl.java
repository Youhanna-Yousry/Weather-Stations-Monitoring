package service.Impl;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import dao.DAO;
import dto.StationStatusMsgDTO;
import service.BaseStationService;

public class BaseStationServiceImpl implements BaseStationService {

    @Inject
    @Named("BitcaskDAO")
    private DAO bitcaskDAO;

    @Override
    public void serveMessage(StationStatusMsgDTO stationStatusMsgDTO) {
        bitcaskDAO.write(stationStatusMsgDTO);
        // TODO save the message to parquet files
    }
}
