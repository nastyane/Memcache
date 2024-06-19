package ru.nastya.memcache.discovery;

import java.io.Serializable;
import java.util.UUID;

public record ClusterNode(UUID id, String host, int port, int communicationPort) implements Serializable {
}
