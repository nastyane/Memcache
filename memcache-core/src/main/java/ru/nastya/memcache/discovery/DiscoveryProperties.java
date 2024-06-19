package ru.nastya.memcache.discovery;

import java.util.List;

public record DiscoveryProperties(List<IpAddress> discoveryAddresses, Range portRange) {
}
