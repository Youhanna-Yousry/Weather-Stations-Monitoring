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
        System.out.println("Received message: " + stationStatusMsgDTO.toString());
        bitcaskDAO.write(stationStatusMsgDTO);
        System.out.println("Message saved to Bitcask " + bitcaskDAO.read(stationStatusMsgDTO.getStationId()));
        // TODO save the message to parquet files
    }
}
