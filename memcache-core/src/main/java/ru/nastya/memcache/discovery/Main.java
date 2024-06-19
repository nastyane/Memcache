package ru.nastya.memcache.discovery;

import ru.nastya.memcache.cache.DistributedCache;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        @SuppressWarnings("resource")
        DistributedCache<String, String> cache = new DistributedCache<>();

        cache.start();
    }
}
