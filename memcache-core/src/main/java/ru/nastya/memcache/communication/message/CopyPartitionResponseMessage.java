package ru.nastya.memcache.communication.message;

import ru.nastya.memcache.cache.Partition;

import java.io.Serializable;

public record CopyPartitionResponseMessage<K extends Serializable, V extends Serializable>(Partition<K, V> partition) implements CommunicationMessage {
}
