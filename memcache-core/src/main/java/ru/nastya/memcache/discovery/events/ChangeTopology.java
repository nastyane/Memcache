package ru.nastya.memcache.discovery.events;

import ru.nastya.memcache.discovery.ClusterNode;

import java.util.List;

public record ChangeTopology(List<ClusterNode> oldTopology, List<ClusterNode> topology) implements DiscoveryEvent {

}