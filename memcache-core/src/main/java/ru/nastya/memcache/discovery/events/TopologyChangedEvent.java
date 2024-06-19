package ru.nastya.memcache.discovery.events;

import ru.nastya.memcache.discovery.ClusterNode;

import java.io.Serializable;
import java.util.*;

public record TopologyChangedEvent(List<ClusterNode> oldTopology, List<ClusterNode> topology,
                                   Map<Serializable, Serializable> nodesPartitions) implements DiscoveryEvent {
}
