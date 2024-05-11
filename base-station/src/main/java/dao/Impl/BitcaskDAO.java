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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class BitcaskDAO implements DAO {

    private static final String BITCASK_BASE_DIRECTORY = "src/main/resources/bitcask";
    private static final int MAX_FILE_SIZE = 1024 * 1024;
    private static final short KEY_SIZE = 8;

    @Named("BitcaskLogger")
    private final Logger logger;
    private final Mapper mapper;
    private final Map<Long, KeyDirValue> keyDir;
    private RandomAccessFile activeFile;
    private long activeFileID;

    @Inject
    public BitcaskDAO(Logger logger, Mapper mapper) {
        this.logger = logger;
        this.mapper = mapper;
        this.keyDir = new ConcurrentHashMap<>();

        if (!new File(BITCASK_BASE_DIRECTORY).exists()) {
            createDirectory();
            logger.info("Created the bitcask directory successfully");
        } else {
            loadHintFiles();
            logger.info("Bitcask directory already exists");
        }

        createActiveFile();
    }

    private void createDirectory() {
        if (!new File(BITCASK_BASE_DIRECTORY).mkdir()) {
            String message = "Failed to create the bitcask directory";
            logger.severe(message);
        }
    }

    private void loadHintFiles() {
        Pattern pattern = Pattern.compile("hint-\\d+");

        for (File file: Objects.requireNonNull(new File(BITCASK_BASE_DIRECTORY).listFiles())) {
            if (!file.isFile() || !pattern.matcher(file.getName()).matches()) {
                continue;
            }
            try (RandomAccessFile hintFile = new RandomAccessFile(file, "r")) {
                while (hintFile.getFilePointer() < hintFile.length()) {
                    byte[] keyDirValueBytes = new byte[KeyDirValue.SIZE];
                    hintFile.read(keyDirValueBytes);

                    KeyDirValue value = new KeyDirValue(keyDirValueBytes);
                    KeyDirValue currentValue = keyDir.get(value.getFileID());

                    if (currentValue == null || currentValue.getTimestamp() < value.getTimestamp()) {
                        keyDir.put(value.getFileID(), value);
                    }
                }
            } catch (IOException e) {
                logger.severe("Failed to read from the hint file");
            }
        }
    }

    private void createActiveFile() {
        try {
            this.activeFileID = System.currentTimeMillis();
            File file = new File(BITCASK_BASE_DIRECTORY + "/" + this.activeFileID);
            this.activeFile = new RandomAccessFile(file, "rws");
        } catch (IOException e) {
            logger.severe("Failed to create the active bitcask file");
        }
    }

    @Override
    public void write(StationStatusMsgDTO stationStatusMsgDTO) {

        byte[] serializedValue;
        try {
            serializedValue = mapper.serializeStationStatusMsg(stationStatusMsgDTO);
        } catch (IOException e) {
            logger.severe("Failed to serialize station status message");
            return;
        }

        try {
            if (activeFile.length() >= MAX_FILE_SIZE) {
                activeFile.close();
                createActiveFile();
            }

            activeFile.seek(activeFile.length());

            long key = stationStatusMsgDTO.getStationId();
            long timestamp = System.currentTimeMillis();

            activeFile.writeLong(timestamp);
            activeFile.writeShort(KEY_SIZE);
            activeFile.writeLong(key);
            activeFile.writeShort(serializedValue.length);
            long offset = activeFile.getFilePointer();
            activeFile.write(serializedValue);

            keyDir.put(key, new KeyDirValue(this.activeFileID, (short) serializedValue.length, offset, timestamp));
        } catch (IOException e) {
            logger.severe("Failed to write to the bitcask file");
            System.exit(1);
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
