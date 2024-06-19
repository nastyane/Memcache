package ru.nastya.memcache.discovery;

import java.util.List;

public record DiscoveryTopologyChangedMessage(List<ClusterNode> topology) implements DiscoveryMessage {

}
