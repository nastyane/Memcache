package ru.nastya.memcache.cache;

import java.io.Serializable;

public interface Partition<K extends Serializable, V extends Serializable> extends Serializable {

    int getPartitionNumber();
    void put(K key, V value);
    V get(K key);
}
