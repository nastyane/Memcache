package ru.nastya.memcache.communication;

import ru.nastya.memcache.communication.message.CommunicationMessage;
import ru.nastya.memcache.discovery.ClusterNode;

import java.io.Closeable;

public interface Communication extends Closeable {
    void send(CommunicationMessage message, ClusterNode node);

    void subscribe(CommunicationListener listener);
    void unsubscribe(CommunicationListener listener);

    void start();
}
