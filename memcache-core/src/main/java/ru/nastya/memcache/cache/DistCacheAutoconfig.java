package ru.nastya.memcache.cache;

import jakarta.annotation.PreDestroy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.Serializable;

@Configuration
public class DistCacheAutoconfig {
    @Bean
    @ConditionalOnMissingBean
    public <K extends Serializable, V extends Serializable> DistributedCache<K, V> distributedCache() throws IOException {
        DistributedCache<K, V> distributedCache = new DistributedCache<>();
        distributedCache.start();

        return distributedCache;

    }

    @PreDestroy
    public <K extends Serializable, V extends Serializable> void stopCache(DistributedCache<K, V> cache) {
        cache.stop();
    }
}
