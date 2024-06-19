package ru.nastya.memcache.discovery.events;

import java.io.Serializable;

public class Affinity {
    private final int partitions;

    public Affinity(int partitions) {
        this.partitions = partitions;
    }

    public <K extends Serializable> void computePartitions(K key) {
        for (int i = 0; i < partitions; i++) {
            int result = Math.abs(key.hashCode()) % partitions;
            System.out.println("Key: " + key + ", Partitions: " + partitions + ", Result: " + result);
        }
    }
}
