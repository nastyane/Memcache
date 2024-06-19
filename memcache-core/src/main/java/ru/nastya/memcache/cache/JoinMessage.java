package ru.nastya.memcache.cache;

import ru.nastya.memcache.discovery.ClusterNode;
import ru.nastya.memcache.discovery.DiscoveryMessage;

public record JoinMessage(ClusterNode node) implements DiscoveryMessage {
}
