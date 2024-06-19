package ru.nastya.memcache.discovery;

import ru.nastya.memcache.discovery.events.ChangeTopology;
import ru.nastya.memcache.discovery.events.DiscoveryEvent;
import ru.nastya.memcache.exception.MemcacheException;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.*;
import java.time.Clock;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

public class TcpNode implements StarTopologyNode {
    // fist - coordinator
    private final List<ClusterNode> topology = new ArrayList<>();
    private final List<ClusterNode> oldTopology = new ArrayList<>();
    private final Object topologyMonitor = new Object();
    private volatile ClusterNode currentNode;
    private volatile boolean isCoordinator;
    private final DiscoveryProperties discoveryProperties; //
    private final ExecutorService connectionPool; //reusing threads
    private final BlockingQueue<DiscoveryMessage> inboundMessages = new LinkedBlockingQueue<>(10); // todo set limit
    private final List<DiscoveryCommunicationNode> communicationNodes = new ArrayList<>();
    private volatile ServerListener serverListener;
    private final Map<UUID, Instant> responseTimeStamp = new ConcurrentHashMap<>();
    private final Clock clock = Clock.systemUTC();

    private final List<DiscoveryListener> listeners = new CopyOnWriteArrayList<>();


    public TcpNode() throws IOException {
        discoveryProperties = loadProperties();
        connectionPool = Executors.newCachedThreadPool();
    }

    //            try {
//                if (nodeAddress == InetAddress.getLocalHost()) {
//                    continue;
//                }
//            } catch (UnknownHostException e) {
//                throw new RuntimeException(e);
//            }
    @Override
    public void start() {
        // 1. Start listen thread.
        startServerThread();
        // 2. In current thread scan for nodes
        boolean connected = false;
        for (IpAddress nodeAddress : discoveryProperties.discoveryAddresses()) {//checking all ports
            if (nodeAddress.port() == currentNode.port()) {
                continue;
            }
            try {
                final Socket socket = new Socket(nodeAddress.host(), nodeAddress.port());
                connected = connect(socket);
                if (connected) {
                    // I'm a regular node, continue
                    System.out.println("[Regular node] Connected to " + nodeAddress);
                    break;
                }
            } catch (ConnectException e) {
                System.err.println(e.getMessage() + ": " + nodeAddress);
            } catch (IOException e) {
                System.err.println("Fail created socket: " + e.getMessage());
            } catch (ClassNotFoundException e) {
                System.out.println("The message could not be implemented" + e.getMessage());
                throw new RuntimeException(e);
            }
        }

        // 3. If no nodes exist, declare myself as coordinator and add to topology
        if (!connected) {
            // I'm the coordinator
            isCoordinator = true;
            final List<ClusterNode> top;
            synchronized (topologyMonitor) {
                topology.add(currentNode);
                top = new ArrayList<>(topology);
            }
            notifyListeners(new ChangeTopology(oldTopology,top));
            System.out.println("[Coordinator] I'm the coordinator now, topology=" + top);
        }
    }

    private boolean connect(Socket socket) throws IOException, ClassNotFoundException {
        final InputStream in = socket.getInputStream();
        final OutputStream out = socket.getOutputStream();

        final ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
        final ObjectInputStream objectInputStream = new ObjectInputStream(in);

        objectOutputStream.writeObject(new DiscoveryJoinMessage(currentNode)); // todo it's null
        final DiscoveryMessage response = (DiscoveryMessage) objectInputStream.readObject();
        final ClusterNode coordinatorNode;
        if (response instanceof DiscoveryReconnectMessage reconnectMessage) {
            closeQuietly(socket);
            final Socket coordinatorSocket = new Socket(
                    reconnectMessage.coordinator().host(),
                    reconnectMessage.coordinator().port()
            );

            return connect(coordinatorSocket);
        } else if (response instanceof DiscoveryJoinedMessage joined) {
            coordinatorNode = joined.topology().getFirst();
            synchronized (topologyMonitor) {
                topology.clear();
                topology.addAll(joined.topology());
                System.out.println("Connected, topology: " + topology);
            }
        } else {
            System.out.println("Error: unknown response type: " + response);
            return false;
        }

        final DiscoveryReader reader = new DiscoveryReader(socket, objectInputStream, coordinatorNode);
        final DiscoveryWriter writer = new DiscoveryWriter(socket, objectOutputStream);
        final ClusterNode coordinator;
        synchronized (topologyMonitor) {
            coordinator = topology.getFirst();
        }
        final DiscoveryCommunicationNode communicationNode = new DiscoveryCommunicationNode(coordinator, reader, writer);

        connectionPool.submit(reader);
        connectionPool.submit(writer);

        synchronized (topologyMonitor) {
            communicationNodes.add(communicationNode);
        }

        return true;
    }

    private void startServerThread() {
        final ServerSocket serverSocket = openPort(discoveryProperties.portRange());
        final ServerListener serverListener = new ServerListener(serverSocket);
        this.serverListener = serverListener;
        this.currentNode = new ClusterNode(
                UUID.randomUUID(),
                serverSocket.getInetAddress().getHostName(),
                serverSocket.getLocalPort(),
                0 // todo pick communication port
                );

        connectionPool.submit(serverListener);
    }

    private ServerSocket openPort(Range portRange) {
        for (int port = portRange.min(); port <= portRange.max(); port++) {
            try {
                final ServerSocket serverSocket = new ServerSocket(port);
                System.out.println("Bound to port " + port);
                return serverSocket;
            } catch (IOException e) {
                System.err.println(e.getMessage() + " " + port);
            }
        }

        throw new MemcacheException("Can't open port in range: " + portRange);
    }

    @Override
    public void stop() {
        connectionPool.shutdownNow();
        closeQuietly(serverListener);
    }

    private DiscoveryProperties loadProperties() throws IOException {
        try (final InputStream propIn = getClass().getClassLoader().getResourceAsStream("memcache.properties")) {
            final Properties props = new Properties();
            props.load(propIn);

            final String addressProperty = props.getProperty("memcache.discovery.address");
            final String portProperty = props.getProperty("memcache.discovery.port-range");
            final String[] addresses = addressProperty.split(",");
            final String[] ports = portProperty.split("-");
            final Range portRange = new Range(Integer.parseInt(ports[0]), Integer.parseInt(ports[1]));
            final List<IpAddress> discoveryAddresses = Arrays.stream(addresses)
                    .map(p -> {
                        final String[] addr = p.split(":");
                        return new IpAddress(addr[0], Integer.parseInt(addr[1]));
                    })
                    .toList();

            return new DiscoveryProperties(discoveryAddresses, portRange);
        }
    }

    private record DiscoveryCommunicationNode(ClusterNode node,
                                              DiscoveryReader reader,
                                              DiscoveryWriter writer) implements Closeable {

        public boolean sendMessage(DiscoveryMessage message) throws InterruptedException {
            return writer.sendMessage(message);
        }


        @Override
        public void close() {
            closeQuietly(reader);
            closeQuietly(writer);
        }
    }

    private class DiscoveryReader implements Runnable, Closeable {
        private final ObjectInputStream in;
        private final Socket socket;
        private final ClusterNode node;

        public DiscoveryReader(Socket socket, ObjectInputStream in, ClusterNode node) throws IOException {
            this.socket = socket;
            this.in = in;
            this.node = node;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final List<ClusterNode> topChange = new ArrayList<>(topology);
                    final DiscoveryMessage message = (DiscoveryMessage) in.readObject();
                    recordMessageReceiveTimestamp(node.id());
                    if (message instanceof DiscoveryTopologyChangedMessage topChanged) {

                        synchronized (topologyMonitor) {
                           //oldTopology.clear();
                           oldTopology.addAll(topChange);
                            topology.clear();
                            topology.addAll(topChanged.topology());
                        }
                        // add
                            notifyListeners(new ChangeTopology(oldTopology, topology));
                        // todo notify listeners
                        System.out.println("[Regular node] Topology changed: " + topChanged.topology());
                    }

                    // todo implement

                } catch (EOFException e) {
                    System.err.println("Node disconnected: " + node + ", " + e.getMessage());
//                    try {
//                        processNodeDisconnected(node);
//                    } catch (InterruptedException ex) {
//                        // ignore
//                    }
                    break;
                } catch (IOException e) {
                    System.err.println("Сouldn't add to topology" + e.getMessage());
                    // e.printStackTrace();
                    try {
                        processNodeDisconnected(node);
                    } catch (InterruptedException ex) {
                        // ignore
                    }
                    break;
                } catch (ClassNotFoundException e) {
                    System.err.println("Class of a serialized object cannot be found." + e.getMessage());
                }
            }

            closeQuietly(this);
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private class DiscoveryWriter implements Runnable, Closeable {
        private final ObjectOutputStream out;
        private final Socket socket;
        private final BlockingQueue<DiscoveryMessage> messages = new LinkedBlockingQueue<>(10); //todo hardcode

        public DiscoveryWriter(Socket socket, ObjectOutputStream out) throws IOException {
            this.socket = socket;
            this.out = out;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final DiscoveryMessage message = messages.take();
                    out.writeObject(message);
                } catch (InterruptedException e) {
                    System.err.println("Interrupted message");
                    closeQuietly(this);
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                }
            }

            closeQuietly(this);
        }

        public boolean sendMessage(DiscoveryMessage message) throws InterruptedException {
            return messages.offer(message, 10, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws IOException {
            closeQuietly(out);
            socket.close();
        }
    }

    private void processNodeDisconnected(ClusterNode disconnectedNode) throws InterruptedException {
        final List<ClusterNode> newTopology;
        if (isCoordinator) {
            synchronized (topologyMonitor) {
                topology.remove(disconnectedNode);
                for (final Iterator<DiscoveryCommunicationNode> it = communicationNodes.iterator(); it.hasNext(); ) {
                    final DiscoveryCommunicationNode communicationNode = it.next();
                    if (communicationNode.node().id().equals(disconnectedNode.id())) {
                        closeQuietly(communicationNode);
                        it.remove();
                        break;
                    }
                }

                newTopology = topology;
            }
            broadcast(new DiscoveryTopologyChangedMessage(newTopology));

            System.out.println("[Coordinator] node left: " + newTopology);
        } else {
            synchronized (topologyMonitor) {
                for (final Iterator<ClusterNode> it = topology.iterator(); it.hasNext(); ) {
                    final ClusterNode nextCoordinator = it.next();
                    if (nextCoordinator.id().equals(disconnectedNode.id())) {
                        it.remove();
                        continue;
                    }

                    if (nextCoordinator.id().equals(currentNode.id())) {
                        // I'm the coordinator
                        isCoordinator = true;
                        System.out.println("Now I'm the coordinator: " + currentNode);
                        break;
                    } else {
                        try {
                            final Socket socket = new Socket(nextCoordinator.host(), nextCoordinator.port());
                            final boolean connected = connect(socket);
                            if (connected) {
                                break;
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                for (final Iterator<DiscoveryCommunicationNode> it = communicationNodes.iterator(); it.hasNext(); ) {
                    final DiscoveryCommunicationNode communicationNode = it.next();
                    if (communicationNode.node().id().equals(disconnectedNode.id())) {
                        closeQuietly(communicationNode);
                        it.remove();
                        break;
                    }
                }
            }
        }
    }

    private class ServerListener implements Runnable, Closeable {
        private final ServerSocket serverSocket;

        public ServerListener(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final Socket socket = serverSocket.accept();

                    final IncomingConnectionProcessor processor = new IncomingConnectionProcessor(socket);
                    connectionPool.submit(processor);
                } catch (IOException e) {
                    System.err.println("Fail created socket run:" + e.getMessage());
                }
            }
            close();
        }

        @Override
        public void close() {
            closeQuietly(serverSocket);
        }

    }

    private class IncomingConnectionProcessor implements Runnable, Closeable {
        private final Socket socket;

        private IncomingConnectionProcessor(Socket socket) {
            this.socket = socket;
        }

        @Override
            public void run() {
            try {
                final ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                final ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

                final DiscoveryMessage message = (DiscoveryMessage) in.readObject();
                if (message instanceof DiscoveryJoinMessage joinMessage) {
                    if (isCoordinator) {
                        final ClusterNode connectingNode = joinMessage.node();

                        recordMessageReceiveTimestamp(connectingNode.id());

                        final List<ClusterNode> top;

                        synchronized (topologyMonitor) {
                            oldTopology.clear();
                            oldTopology.addAll(topology);
                            topology.add(connectingNode);
                            top = new ArrayList<>(topology);

                        }
                        System.out.println("[Coordinator] node joined, topology=" + top);

                        out.writeObject(new DiscoveryJoinedMessage(top));

                        final DiscoveryReader reader = new DiscoveryReader(socket, in, connectingNode);
                        final DiscoveryWriter writer = new DiscoveryWriter(socket, out);

                        final DiscoveryCommunicationNode communicationNode = new DiscoveryCommunicationNode(connectingNode, reader, writer);
                        synchronized (topologyMonitor) {
                            communicationNodes.add(communicationNode);
                        }
                        connectionPool.submit(reader);
                        connectionPool.submit(writer);

                        notifyListeners(new ChangeTopology(oldTopology, top));
                        broadcast(new DiscoveryTopologyChangedMessage(top));



                        //remove
                        // todo notify listeners
                    } else {
                        out.writeObject(new DiscoveryReconnectMessage(topology.getFirst()));
                        closeQuietly(this);
                    }
                }
            } catch (IOException | ClassNotFoundException | InterruptedException e) {
                System.err.println("Failed to connect" + e.getMessage());
                close();
            }


        }


        @Override
        public void close() {
            closeQuietly(socket);
        }
    }

    private void recordMessageReceiveTimestamp(UUID nodeId) {
        responseTimeStamp.put(nodeId, Instant.now(clock));
    }


    private void broadcast(DiscoveryMessage message) throws InterruptedException {
        synchronized (topologyMonitor) {
            for (DiscoveryCommunicationNode communicationNode : communicationNodes) {
                communicationNode.sendMessage(message);
            }
        }
    }

    @Override
    public ClusterNode getCurrentNode() {
        return currentNode;
    }

    @Override
    public void subscribe(DiscoveryListener listener) {
        listeners.add(listener);
    }

    @Override
    public void unsubscribe(DiscoveryListener listener) {
        listeners.remove(listener);
    }

    private void notifyListeners(DiscoveryEvent event) {
        for (DiscoveryListener listener : listeners) {
            listener.onTopologyChanged(event);
        }
    }


    private static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                System.err.println(e.getMessage());
                // ignore
            }
        }
    }

}