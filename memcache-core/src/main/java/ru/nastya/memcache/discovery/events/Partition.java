package ru.nastya.memcache.discovery.events;

import java.io.Serializable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class Partition {
    public class partition implements Serializable {

        private final Map<Serializable, Serializable> values = new ConcurrentHashMap<>();
        public <K extends Serializable, V extends Serializable> partition(final K key, final V value) {
            values.put(key, value);
        }

        @SuppressWarnings("unchecked")
        public <K extends Serializable, V extends Serializable> V get(K key) {
            return (V) values.get(key);
        }

    }
}
