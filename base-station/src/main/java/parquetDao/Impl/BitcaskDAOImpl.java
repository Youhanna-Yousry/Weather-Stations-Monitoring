package dao.Impl;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import dao.BitcaskDAO;
import org.slf4j.Logger;
import utils.KeyDirValue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;

public class BitcaskDAOImpl implements BitcaskDAO {

    private static final String BITCASK_BASE_DIRECTORY = "src/main/resources/bitcask";
    private static final int MAX_FILE_SIZE = 1024 * 1024;
    private static final short KEY_SIZE = 8;
    private static final int MERGE_DELAY = 60 * 1000;
    private static final int MERGE_INTERVAL = 2 * 60 * 1000;

    private final Logger logger;
    private final Map<Long, KeyDirValue> globalKeyDir;
    private RandomAccessFile activeFile;
    private long activeFileID;

    @Inject
    public BitcaskDAOImpl(@Named("BitcaskLogger") Logger logger) {
        this.logger = logger;

        if (!new File(BITCASK_BASE_DIRECTORY).exists()) {
            createDirectory();
            this.globalKeyDir = new HashMap<>();
            logger.info("Created the bitcask directory successfully at: {} ", BITCASK_BASE_DIRECTORY);
        } else {
            this.globalKeyDir = loadKeyDir(new HashSet<>(), new HashSet<>());
            logger.info("Bitcask directory already exists at: {}", BITCASK_BASE_DIRECTORY);
        }

        createActiveFile();
        initMergeTask();
    }

    private void createDirectory() {
        if (!new File(BITCASK_BASE_DIRECTORY).mkdir()) {
            logger.error("Failed to create the bitcask directory");
        }
    }

    private void getCurrentFiles(Set<String> hintFileNames, Set<String> dataFileNames) {
        Pattern pattern = Pattern.compile("hint-\\d+");
        String hintPrefix = "hint-";

        for (File file : Objects.requireNonNull(new File(BITCASK_BASE_DIRECTORY).listFiles())) {
            if (!file.isFile()) {
                continue;
            }
            String fileName = file.getName();
            if (pattern.matcher(fileName).matches()) {
                String fileID = fileName.substring(5);
                hintFileNames.add(fileName);
                dataFileNames.remove(fileID);
            } else {
                if (!hintFileNames.contains(hintPrefix + fileName)) {
                    dataFileNames.add(fileName);
                }
            }
        }
    }

    private Map<Long, KeyDirValue> loadKeyDir(Set<String> hintFileNames, Set<String> dataFileNames) {
        Map<Long, KeyDirValue> keyDir = new HashMap<>();

        getCurrentFiles(hintFileNames, dataFileNames);

        dataFileNames.remove(String.valueOf(activeFileID));

        if (!hintFileNames.isEmpty()) {
            loadHintFiles(hintFileNames, keyDir);
        }

        if (!dataFileNames.isEmpty()) {
            loadDataFiles(dataFileNames, keyDir);
        }

        return keyDir;
    }

    private void loadHintFiles(Set<String> hintFileNames, Map<Long, KeyDirValue> keyDir) {
        for (String hintFileName : hintFileNames) {
            try (RandomAccessFile hintFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/" + hintFileName, "r")) {
                while (hintFile.getFilePointer() < hintFile.length()) {
                    byte[] keyBytes = new byte[KEY_SIZE];
                    byte[] keyDirValueBytes = new byte[KeyDirValue.SIZE];

                    hintFile.read(keyBytes);
                    hintFile.read(keyDirValueBytes);

                    long key = ByteBuffer.wrap(keyBytes).getLong();
                    KeyDirValue value = new KeyDirValue(keyDirValueBytes);

                    updateKeyDir(key, value, keyDir);
                }
            } catch (IOException e) {
                logger.error("Failed to read from the hint file: {}", hintFileName, e);
            }
        }
    }

    private void loadDataFiles(Set<String> dataFileNames, Map<Long, KeyDirValue> keyDir) {
        for (String dataFileName : dataFileNames) {
            try (RandomAccessFile dataFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/" + dataFileName, "r")) {
                while (dataFile.getFilePointer() < dataFile.length()) {
                    long timestamp = dataFile.readLong();

                    dataFile.readShort();
                    long key = dataFile.readLong();

                    short valueSize = dataFile.readShort();
                    long offset = dataFile.getFilePointer();

                    byte[] value = new byte[valueSize];
                    dataFile.read(value);
                    updateKeyDir(key, new KeyDirValue(Long.parseLong(dataFileName), valueSize, offset, timestamp), keyDir);
                }
            } catch (IOException e) {
                logger.error("Failed to read from the data file: {}", dataFileName, e);
            }
        }
    }

    private void createActiveFile() {
        try {
            this.activeFileID = System.currentTimeMillis();
            this.activeFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/" + this.activeFileID, "rws");
        } catch (IOException e) {
            logger.error("Failed to create the active bitcask file", e);
        }
    }

    private void updateKeyDir(long key, KeyDirValue value, Map<Long, KeyDirValue> keyDir) {
        KeyDirValue currentValue = keyDir.get(key);

        if (currentValue == null || currentValue.getTimestamp() < value.getTimestamp()) {
            keyDir.put(key, value);
        }
    }

    private synchronized void syncUpdateKeyDir(long key, KeyDirValue value) {
        updateKeyDir(key, value, globalKeyDir);
    }

    private void writeHintFile(Map<Long, KeyDirValue> keyDir, RandomAccessFile hintFile) throws IOException {
        for (Map.Entry<Long, KeyDirValue> entry : keyDir.entrySet()) {
            byte[] serializedEntry = entry.getValue().serializeEntry(entry.getKey());
            hintFile.write(serializedEntry);
        }
    }

    private void deleteFiles(Set<String> hintFileNames, Set<String> dataFileNames) {
        for (String hintFileName : hintFileNames) {
            File hintFile = new File(BITCASK_BASE_DIRECTORY + "/" + hintFileName);
            dataFileNames.add(hintFileName.substring(5));
            if (!hintFile.delete()) {
                logger.error("Failed to delete the hint file {}", hintFileName);
            }
        }

        for (String dataFileName : dataFileNames) {
            File file = new File(BITCASK_BASE_DIRECTORY + "/" + dataFileName);
            if (!file.delete()) {
                logger.error("Failed to delete the data file {}", dataFileName);
            }
        }
    }

    private void initMergeTask() {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                merge();
            }
        }, MERGE_DELAY, MERGE_INTERVAL);
    }

    private void merge() {
        Set<String> hintFileNames = new HashSet<>();
        Set<String> dataFileNames = new HashSet<>();

        Map<Long, KeyDirValue> keyDir = loadKeyDir(hintFileNames, dataFileNames);

        if (keyDir.isEmpty()) {
            return;
        }

        Iterator<Map.Entry<Long, KeyDirValue>> keyDirIterator = keyDir.entrySet().iterator();

        long fileID = System.currentTimeMillis();
        RandomAccessFile dataFile, hintFile;
        Map<Long, KeyDirValue> newKeyDir = new HashMap<>();

        try {
            dataFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/" + fileID, "rws");
            hintFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/hint-" + fileID, "rws");
        } catch (IOException e) {
            logger.error("Failed to create the new data and hint files during merge", e);
            return;
        }

        do {
            try {
                if (dataFile.getFilePointer() >= MAX_FILE_SIZE) {
                    writeHintFile(newKeyDir, hintFile);

                    newKeyDir.clear();
                    dataFile.close();
                    hintFile.close();

                    if (!keyDirIterator.hasNext()) {
                        continue;
                    }

                    fileID = System.currentTimeMillis();
                    dataFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/" + fileID, "rws");
                    hintFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/hint-" + fileID, "rws");
                } else {
                    Map.Entry<Long, KeyDirValue> entry = keyDirIterator.next();
                    long key = entry.getKey();
                    KeyDirValue value = entry.getValue();

                    dataFile.writeLong(value.getTimestamp());
                    dataFile.writeShort(KEY_SIZE);
                    dataFile.writeLong(key);
                    dataFile.writeShort(value.getValueSize());

                    long offset = dataFile.getFilePointer();

                    byte[] serializedValue = new byte[value.getValueSize()];
                    RandomAccessFile randomAccessFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/" + value.getFileID(), "r");
                    randomAccessFile.seek(value.getValueOffset());
                    randomAccessFile.read(serializedValue);
                    randomAccessFile.close();

                    dataFile.write(serializedValue);

                    newKeyDir.put(key, new KeyDirValue(fileID, value.getValueSize(), offset, value.getTimestamp()));
                }
            } catch (IOException e) {
                logger.error("Failed to close the data and hint files", e);
                return;
            }
        } while (keyDirIterator.hasNext());

        try {
            dataFile.close();
            writeHintFile(newKeyDir, hintFile);
            hintFile.close();
        } catch (IOException e) {
            logger.error("Failed to close the data and hint files", e);
        }

        for (Map.Entry<Long, KeyDirValue> entry : keyDir.entrySet()) {
            syncUpdateKeyDir(entry.getKey(), entry.getValue());
        }

        deleteFiles(hintFileNames, dataFileNames);
    }

    @Override
    public void write(long key, byte[] value) {
        try {
            if (activeFile.getFilePointer() >= MAX_FILE_SIZE) {
                activeFile.close();
                createActiveFile();
            }
            short valueSize = (short) value.length;
            long timestamp = System.currentTimeMillis();

            activeFile.writeLong(timestamp);
            activeFile.writeShort(KEY_SIZE);
            activeFile.writeLong(key);
            activeFile.writeShort(valueSize);
            long offset = activeFile.getFilePointer();
            activeFile.write(value);

            syncUpdateKeyDir(key, new KeyDirValue(this.activeFileID, valueSize, offset, timestamp));
        } catch (IOException e) {
            logger.error("Failed to write to the bitcask file", e);
        }
    }

    @Override
    public byte[] read(long key) {
        KeyDirValue keyDirValue = globalKeyDir.get(key);

        if (keyDirValue == null) {
            logger.info("Key {} not found in the key directory", key);
            return null;
        }

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(BITCASK_BASE_DIRECTORY + "/" + keyDirValue.getFileID(), "r")) {
            randomAccessFile.seek(keyDirValue.getValueOffset());

            byte[] value = new byte[keyDirValue.getValueSize()];
            randomAccessFile.read(value);

            return value;
        } catch (IOException e) {
            logger.error("Failed to read key: {} from the bitcask file", key, e);
            return null;
        }
    }
}
