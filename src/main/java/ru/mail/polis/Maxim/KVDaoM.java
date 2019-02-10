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
import java.util.concurrent.ConcurrentMap;

public class KVDaoM implements KVDao {

    private final DB dataBase;
    private final ConcurrentMap mapList;

    public KVDaoM(File dir) {
        if (!dir.isDirectory() || !dir.exists()) {
            throw new IllegalArgumentException();
        }
        dataBase = DBMaker.fileDB(String.format("%s//db", dir.getAbsolutePath())).make();
        mapList = dataBase.treeMap("data", Serializer.JAVA, Serializer.JAVA).createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        Node valueNode = (Node)mapList.get(new Serialize(key));
        if (valueNode == null || valueNode.milestone) {
            throw new NoSuchElementException();
        }
        return valueNode.val;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        Node node = new Node();
        node.milestone = false;
        node.val = value;
        node.timestamp = System.currentTimeMillis();
        mapList.put(new Serialize(key), node);
    }

    @Override
    public void remove(@NotNull byte[] key) {
        Node valueNode = (Node) mapList.get(new Serialize(key));
        if (valueNode != null) {
            if (valueNode.milestone) {
                mapList.remove(new Serialize(key));
            } else {
                valueNode.milestone = true;
                mapList.put(new Serialize(key), valueNode);
            }
        }
    }

    @Override
    public void close() throws IOException {
        dataBase.close();
    }

    public Node getWithInfo(byte[] key) throws NoSuchElementException {
        Node node = (Node) mapList.get(new Serialize(key));
        if (node == null) {
            throw new NoSuchElementException();
        }
        return node;
    }
}
