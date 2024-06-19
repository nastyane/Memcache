package ru.nastya.memcache.communication.message;

public record CopyPartitionRequestMessage(int partitionNum) implements CommunicationMessage {

}
