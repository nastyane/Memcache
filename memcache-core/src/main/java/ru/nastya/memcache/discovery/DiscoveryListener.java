package ru.nastya.memcache.discovery;

import ru.nastya.memcache.discovery.events.DiscoveryEvent;

public interface DiscoveryListener {
    void onTopologyChanged(DiscoveryEvent event );

}
