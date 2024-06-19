package ru.nastya.memcache.cache;

import ru.nastya.memcache.communication.Communication;
import ru.nastya.memcache.communication.CommunicationListener;
import ru.nastya.memcache.communication.TcpCommunication;
import ru.nastya.memcache.communication.message.CommunicationMessage;
import ru.nastya.memcache.communication.message.CopyPartitionRequestMessage;
import ru.nastya.memcache.communication.message.CopyPartitionResponseMessage;
import ru.nastya.memcache.communication.message.PartitionCopiedAckMessage;
import ru.nastya.memcache.discovery.ClusterNode;
import ru.nastya.memcache.discovery.DiscoveryListener;
import ru.nastya.memcache.discovery.Range;
import ru.nastya.memcache.discovery.TcpNode;
import ru.nastya.memcache.discovery.events.ChangeTopology;
import ru.nastya.memcache.discovery.events.DiscoveryEvent;
import ru.nastya.memcache.exception.MemcacheException;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

public class DistributedCache<K extends Serializable, V extends Serializable> implements Cache<K, V>, DiscoveryListener, CommunicationListener, Closeable {
    private final TcpNode localNode = new TcpNode();
    private List<ClusterNode> topology = new ArrayList<>();
    private List<ClusterNode> oldTopology = new ArrayList<>();
    private final Map<Integer, Partition<K, V>> partitions = new HashMap<>();
    private final Communication communication;
    private final int countPartitions;

    private final List<MessageListener> listeners = new CopyOnWriteArrayList<>();


    public DistributedCache() throws IOException {
        try (final InputStream propertyIn = getClass().getClassLoader().getResourceAsStream("memcache.properties")) {
            localNode.subscribe(this);
            final Properties properties = new Properties();
            properties.load(propertyIn);
            final String partitionsNumProperty = properties.getProperty("memcache.cache.count-partitions");
            this.countPartitions = Integer.parseInt(partitionsNumProperty);
            this.communication = new TcpCommunication(localNode);
            communication.subscribe(this);
        }
    }

    @SuppressWarnings("unchecked")
    private void communicationMessageReceived(CommunicationMessage message, ClusterNode senderNode) {
        // todo
        if (message instanceof CopyPartitionRequestMessage requestMsg) {
            communication.send(new CopyPartitionResponseMessage<>(partitions.get(requestMsg.partitionNum())), senderNode);
        } else if (message instanceof CopyPartitionResponseMessage) {
            final CopyPartitionResponseMessage<K, V> responseMessage = (CopyPartitionResponseMessage<K, V>) message;
            // todo protect with synchronized
            partitions.put(responseMessage.partition().getPartitionNumber(), responseMessage.partition());
            communication.send(new PartitionCopiedAckMessage(responseMessage.partition().getPartitionNumber()), senderNode);
        } else if (message instanceof PartitionCopiedAckMessage ackMessage) {
            // if current node is not owner of partition and target node is owner, then delete
            partitions.remove(ackMessage.partitionNum());
        }
    }

    private void onTopologyChange(List<ClusterNode> oldTopology, List<ClusterNode> topology) {
        // recompute partition location
        this.topology = topology;
        final ClusterNode currentNode = localNode.getCurrentNode();
        if (oldTopology.isEmpty()) {
            // do nothing, I'm the only one
        } else {
            for (int partition = 0; partition < countPartitions; partition++) {
                final ClusterNode node = findNode(partition, topology);
                if (currentNode.equals(node)) {
                    // copy from old partition

                    final ClusterNode oldNode = findNode(partition, oldTopology);
                    if (topology.contains(oldNode)) {
                        // we can copy

                        if (node.equals(oldNode)) {
                            // partition already here, do nothing
                        }
                        // copy from oldNode
                        //currentNode = oldNode.;
                        communication.send(new CopyPartitionRequestMessage(partition), oldNode);
                    } else {
                        // we lost partition
                        System.err.println("Lost partition");

                    }
                }
            }
        }
    }

    private ClusterNode findNode(int partition, List<ClusterNode> topology) {
        return topology.get(partition % topology.size());
    }

    private int findPartition(K key) {
        return Math.abs(key.hashCode() % countPartitions);
    }

    @Override
    public void put(K key, V value) {
        final int partitionNum = findPartition(key);
        final ClusterNode node = findNode(partitionNum, this.topology);

        if (node.equals(localNode.getCurrentNode())) {
            Partition<K, V> partition = partitions.get(partitionNum);
            if (partition == null) {
//                 final Socket openSocket = new Socket();
                //connection(Socket, openSocket);
                // absent if not yet copied or not yet created
                // TODO copy from old node!
                partition = new SimplePartition<>(partitionNum);
                partitions.put(partitionNum, partition);
            }
            partition.put(key, value);
        }
    }


    @Override
    public V get(K key) {
        final int partitionNum = findPartition(key);
        final ClusterNode node = findNode(partitionNum, this.topology);
        if (node.equals(localNode.getCurrentNode())) {
            Partition<K, V> partition = partitions.get(partitionNum);
            if (partition != null) {
                return partition.get(key);
            } else {
                System.err.println("Partition doesn't exist");
                return null;
            }
        }
        return null;
    }

    public void connection(Socket socket) throws IOException, ClassNotFoundException {
        final InputStream in = socket.getInputStream();
        final OutputStream out = socket.getOutputStream();
        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
        final ObjectInputStream objectInputStream = new ObjectInputStream(in);
        //objectOutputStream.writeObject(new JoinMessage());
        final Message response = (Message) objectInputStream.readObject();
        notifyListenersMessage(response);
    }

    private void notifyListenersMessage(Message event) {
        for (MessageListener listener : listeners) {
            listener.connection(event);
        }
    }

    public void start() {
        localNode.start();
    }

    public void stop() {
        localNode.stop();
    }

    @Override
    public void close() throws IOException {
        localNode.unsubscribe(this);
        stop();
    }

    private ServerSocket findPort(Range portRange, List<ClusterNode> oldTopology, List<ClusterNode> topology) {
        for (int i = 1; i < 1025; i++) {
            int numberNodeOldTopology = findNode(i, oldTopology).port();
            int numberNodeTopology = findNode(i, topology).port();
            for (int port = portRange.min(); port <= portRange.max(); port++) {
                if (numberNodeOldTopology == port) {
                    try {
                        final ServerSocket serverSocket = new ServerSocket(port);
                        System.out.println("Node is in the cluster" + port);
                        return serverSocket;
                    } catch (IOException e) {
                        System.err.println(e.getMessage() + "Node is not in the cluster");
                    }
                }

            }
        }


        throw new MemcacheException("Can't open port in range: " + portRange);
    }


    @Override
    public void onTopologyChanged(DiscoveryEvent event) {
        if (event instanceof ChangeTopology topologyEvent) {
            onTopologyChange(topologyEvent.oldTopology(), topologyEvent.topology());
//            topology = topologyEvent.topology();
//            oldTopology = topologyEvent.oldTopology();
        } else {
            throw new UnsupportedOperationException("Impossible");
        }
    }

    @Override
    public void onMessageReceived(CommunicationMessage message, ClusterNode senderNode) {
        communicationMessageReceived(message, senderNode);
    }
}



