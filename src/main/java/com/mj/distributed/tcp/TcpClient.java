package com.mj.distributed.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;

public final class TcpClient {

    private TcpClient() {
    }

    /**
     * One connection attempt. Retry policy is the caller's decision.
     */
    public static Connection connect(String host, int port, Duration timeout, ConnectionListener listener)
            throws IOException {
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(host, port), (int) timeout.toMillis());
            TcpConnection c = new TcpConnection(socket, listener);
            c.start();
            return c;
        } catch (IOException e) {
            socket.close();
            throw e;
        }
    }
}
