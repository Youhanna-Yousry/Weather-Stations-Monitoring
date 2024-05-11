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
        System.out.println("Received message: " + stationStatusMsgDTO);
        bitcaskDAO.write(stationStatusMsgDTO);
        StationStatusMsgDTO test = bitcaskDAO.read(stationStatusMsgDTO.getStationId());
        System.out.println("Read message: " + test);
        // TODO save the message to parquet files
    }
}
