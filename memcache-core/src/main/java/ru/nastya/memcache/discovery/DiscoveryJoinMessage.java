package ru.nastya.memcache.discovery;

public record DiscoveryJoinMessage(ClusterNode node) implements DiscoveryMessage {
}
