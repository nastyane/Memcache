package ru.nastya.memcache.communication.message;

public record PartitionCopiedAckMessage(int partitionNum) implements CommunicationMessage {
}
