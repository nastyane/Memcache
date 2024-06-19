package ru.nastya.memcache.discovery;

import java.io.IOException;

public interface StarTopologyNode {
    void start() throws IOException;
    void stop();

    ClusterNode getCurrentNode();

    void subscribe(DiscoveryListener listener);
    void unsubscribe(DiscoveryListener listener);


}
