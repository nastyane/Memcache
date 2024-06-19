package ru.nastya.memcache.discovery;

import java.util.List;

public record DiscoveryJoinedMessage(List<ClusterNode> topology) implements DiscoveryMessage {
}
