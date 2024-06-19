package ru.nastya.memcache.communication;

import ru.nastya.memcache.cache.Message;
import ru.nastya.memcache.communication.message.CommunicationMessage;
import ru.nastya.memcache.discovery.ClusterNode;

public interface CommunicationCallback {
    void onMessageReceived(CommunicationMessage message, ClusterNode sender);
}
