package ru.nastya.memcache.cache;

import java.io.Serializable;

public interface Cache<K extends Serializable, V extends Serializable> {
    void put(K key, V value);
    V get(K key);
}
