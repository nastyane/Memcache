package ru.nastya.memcache.communication.message;

import ru.nastya.memcache.discovery.ClusterNode;

public record ConnectMessage(ClusterNode node) implements CommunicationMessage {
}
