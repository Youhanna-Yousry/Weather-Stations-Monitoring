package dao.Impl;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import dao.DAO;
import dto.StationStatusMsgDTO;
import mapper.Mapper;
import org.slf4j.Logger;
import utils.KeyDirValue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
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
        this.keyDir = new HashMap<>();

        if (!new File(BITCASK_BASE_DIRECTORY).exists()) {
            createDirectory();
            logger.info("Created the bitcask directory successfully");
        } else {
            loadKeyDir();
            logger.info("Bitcask directory already exists");
        }

        createActiveFile();
    }

    private void createDirectory() {
        if (!new File(BITCASK_BASE_DIRECTORY).mkdir()) {
            logger.error("Failed to create the bitcask directory");
        }
    }

    private void loadKeyDir() {
        Pattern pattern = Pattern.compile("hint-\\d+");
        Set<String> hintFileNames = new HashSet<>();
        Set<String> dataFileNames = new HashSet<>();

        for (File file: Objects.requireNonNull(new File(BITCASK_BASE_DIRECTORY).listFiles())) {
            if (!file.isFile()) {
                continue;
            }
            String fileName = file.getName();
            if (pattern.matcher(fileName).matches()) {
                String fileID = fileName.substring(5);
                hintFileNames.add(fileName);
                dataFileNames.remove(fileID);
            } else {
                if (!hintFileNames.contains("hint-" + fileName)) {
                    dataFileNames.add(fileName);
                }
            }
        }

        loadHintFiles(hintFileNames);
        loadDataFiles(dataFileNames);
    }

    private void loadHintFiles(Set<String> hintFileNames) {
        for (String hintFileName: hintFileNames) {
            try (RandomAccessFile hintFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/" + hintFileName, "r")) {
                while (hintFile.getFilePointer() < hintFile.length()) {
                    byte[] keyBytes = new byte[KEY_SIZE];
                    byte[] keyDirValueBytes = new byte[KeyDirValue.SIZE];

                    hintFile.read(keyBytes);
                    hintFile.read(keyDirValueBytes);

                    long key = ByteBuffer.wrap(keyBytes).getLong();
                    KeyDirValue value = new KeyDirValue(keyDirValueBytes);

                    updateKeyDir(key, value);
                }
            } catch (IOException e) {
                logger.error("Failed to read from the hint file");
            }
        }
    }

    private void loadDataFiles(Set<String> dataFileNames) {
        for (String dataFileName: dataFileNames) {
            try (RandomAccessFile dataFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/" + dataFileName, "r")) {
                while (dataFile.getFilePointer() < dataFile.length()) {
                    long timestamp = dataFile.readLong();

                    short keySize = dataFile.readShort();
                    long key = dataFile.readLong();

                    short valueSize = dataFile.readShort();
                    long offset = dataFile.getFilePointer();

                    byte[] value = new byte[valueSize];
                    dataFile.read(value);
                    updateKeyDir(key, new KeyDirValue(Long.parseLong(dataFileName), valueSize, offset, timestamp));
                }
            } catch (IOException e) {
                logger.error("Failed to read from the data file", e);
            }
        }
    }

    private void createActiveFile() {
        try {
            this.activeFileID = System.currentTimeMillis();
            File file = new File(BITCASK_BASE_DIRECTORY + "/" + this.activeFileID);
            this.activeFile = new RandomAccessFile(file, "rws");
        } catch (IOException e) {
            logger.error("Failed to create the active bitcask file", e);
        }
    }

    private synchronized void updateKeyDir(long key, KeyDirValue value) {
        KeyDirValue currentValue = keyDir.get(key);

        if (currentValue == null || currentValue.getTimestamp() < value.getTimestamp()) {
            keyDir.put(key, value);
        }
    }

    @Override
    public void write(StationStatusMsgDTO stationStatusMsgDTO) {
        byte[] serializedValue;
        try {
            serializedValue = mapper.serializeStationStatusMsg(stationStatusMsgDTO);
        } catch (IOException e) {
            logger.error("Failed to serialize station status message", e);
            return;
        }

        try {
            if (activeFile.getFilePointer() >= MAX_FILE_SIZE) {
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

            updateKeyDir(key, new KeyDirValue(this.activeFileID, (short) serializedValue.length, offset, timestamp));
        } catch (IOException e) {
            logger.error("Failed to write to the bitcask file", e);
        }

    }

    @Override
    public StationStatusMsgDTO read(long stationId) {
        KeyDirValue keyDirValue = keyDir.get(stationId);

        if (keyDirValue == null) {
            logger.info("Key {} not found in the key directory", stationId);
            return null;
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/" + keyDirValue.getFileID(), "r")) {
            randomAccessFile.seek(keyDirValue.getValueOffset());

            byte[] value = new byte[keyDirValue.getValueSize()];
            randomAccessFile.read(value);

            return mapper.deserializeStationStatusMsg(value);
        } catch (IOException e) {
            logger.error("Failed to read from the bitcask file", e);
            return null;
        }
    }
}
