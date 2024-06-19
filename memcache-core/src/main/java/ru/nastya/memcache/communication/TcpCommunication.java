package ru.nastya.memcache.communication;

import ru.nastya.memcache.communication.message.CommunicationMessage;
import ru.nastya.memcache.communication.message.ConnectMessage;
import ru.nastya.memcache.discovery.ClusterNode;
import ru.nastya.memcache.discovery.DiscoveryListener;
import ru.nastya.memcache.discovery.TcpNode;
import ru.nastya.memcache.discovery.events.ChangeTopology;
import ru.nastya.memcache.discovery.events.DiscoveryEvent;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TcpCommunication implements Communication, DiscoveryListener {
    private final List<CommunicationListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<ClusterNode, CommunicationPair> communicationPairs = new ConcurrentHashMap<>();
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private volatile List<ClusterNode> topology = List.of();
    private final TcpNode localNode;
    private final Server server = new Server();

    public TcpCommunication(TcpNode localNode) {
        this.localNode = localNode;
        localNode.subscribe(this);
    }

    @Override
    public void send(CommunicationMessage message, ClusterNode node) {
        try {
            CommunicationPair communicationPair = communicationPairs.get(node);
            if (communicationPair == null) {
                connect(node);
            }

            communicationPair = communicationPairs.get(node);
            communicationPair.writer.send(message);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() {
        pool.submit(server);
    }

    @Override
    public void subscribe(CommunicationListener listener) {
        listeners.add(listener);
    }

    @Override
    public void unsubscribe(CommunicationListener listener) {
        listeners.remove(listener);
    }
    
    void connect(ClusterNode node) throws IOException {
        final Socket socket = new Socket(node.host(), node.communicationPort());
        final Reader reader = new Reader(socket, new ObjectInputStream(socket.getInputStream()), node);
        final Writer writer = new Writer(socket);
        
        pool.submit(reader);
        writer.send(new ConnectMessage(localNode.getCurrentNode()));
        
        communicationPairs.put(node, new CommunicationPair(reader, writer));
    }

    @Override
    public void close() throws IOException {
        pool.shutdownNow();
    }

    @Override
    public void onTopologyChanged(DiscoveryEvent event) {
        if (event instanceof ChangeTopology topologyEvent) {
            this.topology = topologyEvent.topology();
        }
    }

    private class Server implements Runnable, Closeable {
        private volatile ServerSocket serverSocket;

        @Override
        public void run() {
            try {
                serverSocket = new ServerSocket(1234); // todo find port from range like in discovery
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        final Socket socket = serverSocket.accept();
                        final ObjectInputStream in  = new ObjectInputStream(socket.getInputStream());
                        final CommunicationMessage message = (CommunicationMessage) in.readObject();
                        if (message instanceof ConnectMessage connectMessage) {
                            final Reader reader = new Reader(socket, in, connectMessage.node());
                            communicationPairs.put(connectMessage.node(), new CommunicationPair(
                                    reader,
                                    new Writer(socket)
                            ));

                            pool.submit(reader);
                        }

                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws IOException {
            if (serverSocket != null) {
                serverSocket.close();
            }
        }
    }

    private static class Writer implements Closeable {
        private final Socket socket;
        private final ObjectOutputStream out;

        private Writer(Socket socket) throws IOException {
            this.socket = socket;
            out = new ObjectOutputStream(socket.getOutputStream());
        }

        public void send(CommunicationMessage message) throws IOException {
            out.writeObject(message);
        }
        
        @Override
        public void close() throws IOException {
            if (socket != null) {
                socket.close();
            }
        }
    }
    
    private class Reader implements Runnable, Closeable {
        private final Socket socket;
        private final ObjectInputStream in;
        private final ClusterNode node;
        
        public Reader(Socket socket, ObjectInputStream in, ClusterNode node) throws IOException {
            this.socket = socket;
            this.in = in;
            this.node = node;
        }

        
        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    final CommunicationMessage message = (CommunicationMessage) in.readObject();
                    for (CommunicationListener listener : listeners) {
                        listener.onMessageReceived(message, node);
                    }
                } catch (Exception e) {
                    // todo catch connection closed exception
                    throw new RuntimeException(e);
                } 
            }
        }
        
        @Override
        public void close() throws IOException {
            if (socket != null) {
                socket.close();
            }
        }
    }
    
    private record CommunicationPair(Reader reader, Writer writer) {}
}
