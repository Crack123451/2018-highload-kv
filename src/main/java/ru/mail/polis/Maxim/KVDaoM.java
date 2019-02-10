package ru.mail.polis.Maxim;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBException;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

public class KVDaoM implements KVDao {

    private final DB dataBase;
    private final Map<byte[], byte[]> mapList;

    public KVDaoM(File dir) {
        if (!dir.isDirectory() || !dir.exists()) {
            throw new IllegalArgumentException();
        }
        dataBase = DBMaker.fileDB(String.format("%s//db", dir.getAbsolutePath())).make();
        mapList = dataBase.treeMap("data", Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY).createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        byte[] bytes = mapList.get(key);
        if (bytes == null) {
            throw new NoSuchElementException();
        }
        return bytes;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        try {
            mapList.put(key, value);
        } catch (DBException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        try {
            mapList.remove(key);
        } catch (DBException ex){
            throw new IOException(ex);
        }
    }

    @Override
    public void close() throws IOException {
        dataBase.close();
    }
}