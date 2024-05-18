package parquetDao;

import dto.StationStatusMsgDTO;

// NOTE: This is an initial version of the interface. It is subject to change.
public interface DAO {

    void writeToParquet(StationStatusMsgDTO stationStatusMsgDTO);
}
