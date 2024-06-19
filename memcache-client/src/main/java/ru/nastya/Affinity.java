package ru.nastya;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class Affinity implements Partitions {
    private final int partitionsPrevious;
    private final int partitionsNow;
    Map<Serializable, Serializable> values = new ConcurrentHashMap<>();

    public Affinity(int partitionPrev, int partitionNow) {
        this.partitionsPrevious = partitionPrev;
        this.partitionsNow = partitionNow;
    }
    public void PartitionsCountNow(int partitionNow){
        for (int i = 0; i < 10; i++) {
            int numberNode = getPartitions(i, partitionNow); //
            if (numberNode == partitionsNow) {
                values.put(i, (Serializable) numberNode);

            }
        }

    }
    public void getPartitions(int partitionPrev){
        for (int i = 0; i < 10; i++) {
            getPartitions(i, partitionPrev);
            // if ()
        }
    }


    public void connection() throws IOException, ClassNotFoundException {
        //todo подключение к ноде где находится partition
        final Socket socket = new Socket();
        final InputStream in = socket.getInputStream();
        final OutputStream out = socket.getOutputStream();

        final ObjectOutputStream objectOutputStreamGetPartition = new ObjectOutputStream(out);
        final ObjectInputStream objectInputStreamGetPartition = new ObjectInputStream(in);
        //Partitions partitions = new SomePartitions();
       // objectOutputStreamGetPartition.writeObject(getValue(key));
        final Partitions response = (Partitions) objectInputStreamGetPartition.readObject();
//        if (response instanceof MessageFoundPartition) {
//
//        } else {
//            System.out.println("Error: Partition not found" + response);
//
//        }
    }

    public <K extends Serializable> int getPartitions(K key, int partitionCount) {
        int result = Math.abs(key.hashCode()) % partitionCount;
        System.out.println("Key: " + key + ", Partitions: " + partitionCount + ", Result: " + result);
        return result;
    }

    private Properties properties;

    public void Partitions(Properties properties) {
        this.properties = properties;
    }

    @Override
    public Properties getProperties() {
        return null;
    }

    @Override
    public Serializable putValue(Serializable key, Serializable value) {
        return values.put(key, value);
    }

    @Override
    public Serializable getValue(Serializable key) {
        return values.get(key);
    }
}
