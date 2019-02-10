package ru.mail.polis.Maxim;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

public class Serialize implements Externalizable, Comparable<Serialize> {
    private transient ByteBuffer buff;

    public Serialize() { }

    public ByteBuffer getBuff() {
        return buff;
    }

    Serialize(byte[] a) {
        buff = ByteBuffer.wrap(a);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        buff = ByteBuffer.wrap((byte[]) in.readObject());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(buff.array());
    }

    @Override
    public int compareTo(@NotNull Serialize o) {
        return buff.compareTo(o.getBuff());
    }
}
