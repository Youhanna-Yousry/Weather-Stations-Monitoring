package dao;

import dto.StationStatusMsgDTO;

public interface ParquetDAO {

    void writeToParquet(StationStatusMsgDTO stationStatusMsgDTO);
}
