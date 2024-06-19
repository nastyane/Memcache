package ru.nastya.memcache.discovery;

public record DiscoveryReconnectMessage(ClusterNode coordinator) implements DiscoveryMessage {
}
