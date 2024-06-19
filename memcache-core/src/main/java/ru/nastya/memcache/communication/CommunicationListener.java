package ru.nastya.memcache.communication;

import ru.nastya.memcache.communication.message.CommunicationMessage;
import ru.nastya.memcache.discovery.ClusterNode;

public interface CommunicationListener {
    void onMessageReceived(CommunicationMessage message, ClusterNode senderNode);
}
