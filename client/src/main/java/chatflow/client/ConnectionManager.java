package chatflow.client;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import jakarta.websocket.ContainerProvider;
import jakarta.websocket.DeploymentException;
import jakarta.websocket.WebSocketContainer;

public class ConnectionManager {
    public final Queue<ClientSendEndPoint> sendConnections = new ConcurrentLinkedQueue<>();
    public final Map<String, ClientReceiveEndPoint> receiveConnections = new ConcurrentHashMap<>();
    private final String sendServerIp;
    private final String receiveServerIp;
    private final int numSendConn;

    public ConnectionManager(
            String sendServerIp, String receiveServerIp, int numSendConn) {
        this.sendServerIp = sendServerIp;
        this.receiveServerIp = receiveServerIp;
        this.numSendConn = numSendConn;
    }

    public void init() {
        for (int i = 0; i < numSendConn; i++) {
            try {
                WebSocketContainer container = ContainerProvider.getWebSocketContainer();
                ClientSendEndPoint client = new ClientSendEndPoint();
                container.connectToServer(client, URI.create("ws://" + sendServerIp + ":8080/send"));
                sendConnections.add(client);
                Statistics.sendConnection.incrementAndGet();
            } catch (DeploymentException | IOException e) {
                System.err.println("Failed to open send connection #" + i + ": " + e.getMessage());
            }
        }
        for (int i = 1; i < 21; i++) {
            try {
                WebSocketContainer container = ContainerProvider.getWebSocketContainer();
                ClientReceiveEndPoint client = new ClientReceiveEndPoint();
                container.connectToServer(
                        client, URI.create("ws://" + receiveServerIp + ":9090/receive/room/" + i));
                receiveConnections.put(String.valueOf(i), client);
                Statistics.receiveConnection.incrementAndGet();
            } catch (DeploymentException | IOException e) {
                System.err.println("Failed to open receive connection for room " + i + ": " + e.getMessage());
            }
        }
        System.out.println("Connections established — send: " + sendConnections.size()
                + "/" + numSendConn + ", receive: " + receiveConnections.size() + "/20");
    }

    public synchronized void addOneSendConn() {
        try {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            ClientSendEndPoint client = new ClientSendEndPoint();
            container.connectToServer(client, URI.create("ws://" + sendServerIp + ":8080/send"));
            sendConnections.add(client);
            notifyAll();
        } catch (DeploymentException | IOException e) {
            System.err.println("Failed to add send connection: " + e.getMessage());
        }
    }

    // private ClientReceiveEndPoint getOneReceConn(String roomId) {
    //     try {
    //         WebSocketContainer container = ContainerProvider.getWebSocketContainer();
    //         ClientReceiveEndPoint client = new ClientReceiveEndPoint();
    //         container.connectToServer(
    //                 client, URI.create("ws://" + receiveServerIp + ":8080/receive/1/room/" + roomId));
    //         return client;
    //     } catch (DeploymentException | IOException e) {
    //         return null;
    //     }
    // }

    public synchronized ClientSendEndPoint borrowSendConn() {
        ClientSendEndPoint conn = null;
        try {
            while (sendConnections.isEmpty()) {
                wait();
            }
            conn = sendConnections.poll();
            notifyAll();
        } catch (InterruptedException e) {
        }
        return conn;
    }

    public synchronized void returnSendConn(ClientSendEndPoint conn) {
        sendConnections.add(conn);
        notifyAll();
    }

    // public synchronized ClientReceiveEndPoint borrowReceConn(String roomId) {
    //     ClientReceiveEndPoint conn = null;
    //     try {
    //         Queue<ClientReceiveEndPoint> queue = receiveConnections.computeIfAbsent(roomId,
    //                 k -> new ConcurrentLinkedQueue<>());
    //         while (queue.isEmpty()) {
    //             wait();
    //         }
    //     } catch (InterruptedException e) {
    //     }
    //     return conn;
    // }

    // public synchronized boolean connectToSendIfClosed() {
    // if (sendConnection == null || !sendConnection.isOpen()) {
    // if (sendConnection == null) {
    // Statistics.sendConnection.incrementAndGet();
    // } else {
    // Statistics.sendReconnection.incrementAndGet();
    // }
    // connectToSendServer();
    // return true;
    // }
    // return false;
    // }

    public boolean connectToReceiveIfClosed(String roomId) {
        if (!receiveConnections.containsKey(roomId) || !receiveConnections.get(roomId).isOpen()) {
            if (!receiveConnections.containsKey(roomId)) {
                Statistics.receiveConnection.incrementAndGet();
            } else {
                Statistics.receiveReconnection.incrementAndGet();
            }
            connectToReceiveServer(roomId);
            return true;
        }
        return false;
    }

    private void connectToReceiveServer(String roomId) {
        try {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            ClientReceiveEndPoint client = new ClientReceiveEndPoint();
            container.connectToServer(
                    client, URI.create("ws://" + receiveServerIp + ":9090/receive/room/" + roomId));
            receiveConnections.put(roomId, client);
        } catch (DeploymentException | IOException e) {
            System.err.println("Failed to reconnect receive room " + roomId + ": " + e.getMessage());
        }
    }

    // private void connectToSendServer() {
    // try {
    // WebSocketContainer container = ContainerProvider.getWebSocketContainer();
    // ClientSendEndPoint client = new ClientSendEndPoint();
    // container.connectToServer(client, URI.create("ws://" + sendServerIp +
    // "/send"));
    // sendConnection = client;
    // } catch (DeploymentException | IOException e) {
    // }
    // }

    public void cleanup() {
        for (ClientSendEndPoint conn : sendConnections) {
            conn.close();
        }
        for(ClientReceiveEndPoint conn:receiveConnections.values()){
                conn.close();
            }
    }
}
