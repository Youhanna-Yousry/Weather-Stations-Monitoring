package service.Impl;

import parquetDao.ArchiveToParquet;
import parquetDao.DAO;
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
    @Named("BaseStationServiceLogger")
    Logger logger;
    @Inject
    Mapper mapper;
    @Inject
    private BitcaskDAO bitcaskDAO;

    @Override
    public void serveMessage(StationStatusMsgDTO stationStatusMsgDTO) {

        // save the message to bitcask
        long key = stationStatusMsgDTO.getStationId();
        byte[] value = null;
        try {
            value = mapper.serializeStationStatusMsg(stationStatusMsgDTO);
        } catch (IOException e) {
            logger.error("Failed to serialize message: {}", stationStatusMsgDTO, e);
        }

        bitcaskDAO.write(key, value);

        // save the message to parquet files
        DAO archive = new ArchiveToParquet();
        archive.writeToParquet(stationStatusMsgDTO);
    }
}
