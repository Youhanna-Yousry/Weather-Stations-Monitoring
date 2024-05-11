package utils;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.nio.ByteBuffer;

@AllArgsConstructor
@Getter
public class KeyDirValue {

    private long fileID;
    private short valueSize;
    private long valueOffset;
    private long timestamp;

    public static final int SIZE = (Short.SIZE  + 3*Long.SIZE) / Byte.SIZE;

    public KeyDirValue(byte[] bytes) {
        byte[] fileIDBytes = new byte[8];
        byte[] valueSizeBytes = new byte[2];
        byte[] valueOffsetBytes = new byte[8];
        byte[] timestampBytes = new byte[8];

        System.arraycopy(bytes, 0, fileIDBytes, 0, fileIDBytes.length);
        System.arraycopy(bytes, fileIDBytes.length, valueSizeBytes, 0, valueSizeBytes.length);
        System.arraycopy(bytes, fileIDBytes.length + valueSizeBytes.length, valueOffsetBytes, 0, valueOffsetBytes.length);
        System.arraycopy(bytes, fileIDBytes.length + valueSizeBytes.length + valueOffsetBytes.length, timestampBytes, 0, timestampBytes.length);

        fileID = ByteBuffer.wrap(fileIDBytes).getLong();
        valueSize = ByteBuffer.wrap(valueSizeBytes).getShort();
        valueOffset = ByteBuffer.wrap(valueOffsetBytes).getLong();
        timestamp = ByteBuffer.wrap(timestampBytes).getLong();
    }


    private byte[] serializeEntry(long key) {
        byte[] keyBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(key).array();
        byte[] fileIDBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(fileID).array();
        byte[] valueSizeBytes = ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(valueSize).array();
        byte[] valueOffsetBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(valueOffset).array();
        byte[] timestampBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(timestamp).array();

        byte[] result = new byte[keyBytes.length + fileIDBytes.length + valueSizeBytes.length + valueOffsetBytes.length + timestampBytes.length];

        System.arraycopy(keyBytes, 0, result, 0, keyBytes.length);
        System.arraycopy(fileIDBytes, 0, result, keyBytes.length, fileIDBytes.length);
        System.arraycopy(valueSizeBytes, 0, result, keyBytes.length + fileIDBytes.length, valueSizeBytes.length);
        System.arraycopy(valueOffsetBytes, 0, result, keyBytes.length + fileIDBytes.length + valueSizeBytes.length, valueOffsetBytes.length);
        System.arraycopy(timestampBytes, 0, result, keyBytes.length + fileIDBytes.length + valueSizeBytes.length + valueOffsetBytes.length, timestampBytes.length);

        return result;
    }
}
