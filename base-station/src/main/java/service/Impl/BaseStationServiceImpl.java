package service.Impl;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import dao.BitcaskDAO;
import dto.StationStatusMsgDTO;
import mapper.Mapper;
import org.slf4j.Logger;
import service.BaseStationService;

import java.io.IOException;

public class BaseStationServiceImpl implements BaseStationService {

    @Inject
    private BitcaskDAO bitcaskDAO;

    @Inject
    Mapper mapper;

    @Inject
    @Named("BaseStationServiceLogger")
    Logger logger;

    @Override
    public void serveMessage(StationStatusMsgDTO stationStatusMsgDTO) {
        long key = stationStatusMsgDTO.getStationId();
        byte[] value = null;

        try {
            value = mapper.serializeStationStatusMsg(stationStatusMsgDTO);
        } catch (IOException e) {
            logger.error("Failed to serialize message: {}", stationStatusMsgDTO, e);
        }

        bitcaskDAO.write(key, value);

        // TODO save the message to parquet files
    }
}
