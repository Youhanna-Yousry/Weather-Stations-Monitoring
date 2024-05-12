package dao;


public interface BitcaskDAO {

    void write(long key, byte[] value);

    byte[] read(long key);
}
