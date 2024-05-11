package dao.Impl;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import dao.DAO;
import dto.StationStatusMsgDTO;
import mapper.Mapper;
import utils.KeyDirValue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class BitcaskDAO implements DAO {

    private static final String BITCASK_BASE_DIRECTORY = "src/main/resources/bitcask";
    private static final int MAX_FILE_SIZE = 1024 * 1024;
    private static final short KEY_SIZE = 8;

    @Named("BitcaskLogger")
    private final Logger logger;
    private final Mapper mapper;
    private final Map<Long, KeyDirValue> keyDir;
    private File activeFile;

    @Inject
    public BitcaskDAO(Logger logger, Mapper mapper) {
        this.logger = logger;
        this.mapper = mapper;
        this.keyDir = new ConcurrentHashMap<>();

        if (!new File(BITCASK_BASE_DIRECTORY).exists()) {
            createDirectory();
            logger.info("Created the bitcask directory successfully");
        } else {
            // TODO: Read the existing hint files and load them into memory
            logger.info("Bitcask directory already exists");
        }

        activeFile = new File(BITCASK_BASE_DIRECTORY + "/" + System.currentTimeMillis());
    }

    private void createDirectory() {
        if (!new File(BITCASK_BASE_DIRECTORY).mkdir()) {
            String message = "Failed to create the bitcask directory";
            logger.severe(message);
            throw new RuntimeException(message);
        }
    }

    @Override
    public void write(StationStatusMsgDTO stationStatusMsgDTO) {
        if (activeFile.length() >= MAX_FILE_SIZE) {
            activeFile = new File(BITCASK_BASE_DIRECTORY + "/" + System.currentTimeMillis());
        }

        byte[] serializedValue;
        try {
            serializedValue = mapper.serializeStationStatusMsg(stationStatusMsgDTO);
        } catch (IOException e) {
            logger.severe("Failed to serialize station status message");
            return;
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(activeFile, "rw")) {
            long key = stationStatusMsgDTO.getStationId();
            long timestamp = System.currentTimeMillis();

            randomAccessFile.writeLong(timestamp);
            randomAccessFile.writeShort(KEY_SIZE);
            randomAccessFile.writeLong(key);
            randomAccessFile.writeShort(serializedValue.length);
            long offset = randomAccessFile.getFilePointer();
            randomAccessFile.write(serializedValue);

            keyDir.put(key, new KeyDirValue(activeFile.getName(), (short) serializedValue.length, offset, timestamp));
        } catch (IOException e) {
            logger.severe("Failed to write to the bitcask file");
        }

    }

    @Override
    public StationStatusMsgDTO read(long stationId) {
        KeyDirValue keyDirValue = keyDir.get(stationId);

        if (keyDirValue == null) {
            logger.info("Key " + stationId + " not found in the key directory");
            return null;
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/" + keyDirValue.getFileID(), "r")) {
            randomAccessFile.seek(keyDirValue.getValueOffset());

            byte[] value = new byte[keyDirValue.getValueSize()];
            randomAccessFile.read(value);

            return mapper.deserializeStationStatusMsg(value);
        } catch (IOException e) {
            logger.severe("Failed to read from the bitcask file");
            return null;
        }
    }
}
