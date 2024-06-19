package ru.nastya.memcache.cache;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SimplePartition<K extends Serializable, V extends Serializable> implements Partition<K, V> {
    private final Map<K, V> values = new HashMap<K, V>();
    private final int partitionNumber;

    public SimplePartition(int partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    @Override
    public int getPartitionNumber() {
        return partitionNumber;
    }

    @Override
    public void put(K key, V value) {
        values.put(key, value);
    }

    @Override
    public V get(K key) {
        return values.get(key);
    }
}
