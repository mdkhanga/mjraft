package com.mj.raft.testutil;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

public final class Ports {

    private Ports() {
    }

    /**
     * Returns n distinct ports that were free at the time of the call.
     * All sockets are held open together so the OS cannot hand out the same port twice.
     */
    public static int[] free(int n) throws IOException {
        List<ServerSocket> sockets = new ArrayList<>();
        try {
            for (int i = 0; i < n; i++) {
                ServerSocket s = new ServerSocket(0);
                s.setReuseAddress(true);
                sockets.add(s);
            }
            return sockets.stream().mapToInt(ServerSocket::getLocalPort).toArray();
        } finally {
            for (ServerSocket s : sockets) {
                s.close();
            }
        }
    }
}
