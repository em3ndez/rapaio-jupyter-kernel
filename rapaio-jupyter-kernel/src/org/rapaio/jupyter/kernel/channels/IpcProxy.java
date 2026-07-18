package org.rapaio.jupyter.kernel.channels;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.rapaio.jupyter.kernel.core.ConnectionProperties;

/**
 * Proxy which allows the kernel to serve connection files using the {@code ipc} transport
 * (Unix domain sockets), even though the underlying JeroMQ library only supports {@code tcp}.
 * <p>
 * Jupyter servers which use ipc transport (for example Google Colab) expect the kernel to bind
 * Unix domain sockets at paths of the form {@code <ip>-<port>}, where {@code ip} is the base
 * path from the connection file and {@code port} is the channel port number (see
 * {@code jupyter_client} connection address format {@code ipc://<ip>-<port>}).
 * <p>
 * Since the ZMTP wire protocol is byte-identical over TCP and Unix domain sockets, this proxy
 * binds a Unix domain socket for each channel and pumps raw bytes between accepted connections
 * and a loopback TCP port where the kernel's own ZMQ socket is bound. The rest of the kernel
 * operates unchanged on an equivalent {@code tcp} connection specification returned by
 * {@link #start()}.
 */
public final class IpcProxy {

    private static final Logger LOGGER = Logger.getLogger(IpcProxy.class.getSimpleName());

    // maximum length of a unix domain socket path is platform dependent (usually 104-108 bytes),
    // we use a conservative limit only to produce a friendly error message
    private static final int MAX_UDS_PATH_LENGTH = 100;

    private final ConnectionProperties ipcProps;
    private final List<ChannelProxy> proxies = new ArrayList<>();
    private volatile boolean started = false;
    private volatile boolean closed = false;

    public IpcProxy(ConnectionProperties ipcProps) {
        this.ipcProps = ipcProps;
    }

    /**
     * Starts a Unix domain socket listener for each channel and returns an equivalent
     * connection specification using tcp transport over loopback, on which the kernel
     * ZMQ sockets should bind.
     *
     * @return connection properties with tcp transport to be used by the kernel channels
     * @throws IOException if the unix domain sockets cannot be created
     */
    public synchronized ConnectionProperties start() throws IOException {
        if (started) {
            throw new IllegalStateException("Proxy already started.");
        }

        int controlPort = allocateFreePort();
        int shellPort = allocateFreePort();
        int stdinPort = allocateFreePort();
        int hbPort = allocateFreePort();
        int iopubPort = allocateFreePort();

        // validate all socket paths before creating any listener
        Path controlPath = socketPath(ipcProps.controlPort());
        Path shellPath = socketPath(ipcProps.shellPort());
        Path stdinPath = socketPath(ipcProps.stdinPort());
        Path hbPath = socketPath(ipcProps.hbPort());
        Path iopubPath = socketPath(ipcProps.iopubPort());

        try {
            proxies.add(new ChannelProxy("control", controlPath, controlPort));
            proxies.add(new ChannelProxy("shell", shellPath, shellPort));
            proxies.add(new ChannelProxy("stdin", stdinPath, stdinPort));
            proxies.add(new ChannelProxy("hb", hbPath, hbPort));
            proxies.add(new ChannelProxy("iopub", iopubPath, iopubPort));
        } catch (IOException e) {
            close();
            throw e;
        }
        proxies.forEach(ChannelProxy::startAccepting);
        started = true;

        return new ConnectionProperties(controlPort, shellPort, "tcp", ipcProps.signatureScheme(),
                stdinPort, hbPort, "127.0.0.1", iopubPort, ipcProps.key());
    }

    /**
     * Closes all listeners and active connections and removes the socket files.
     */
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        proxies.forEach(ChannelProxy::close);
    }

    private Path socketPath(int port) {
        String path = ipcProps.ip() + "-" + port;
        if (path.getBytes().length > MAX_UDS_PATH_LENGTH) {
            throw new IllegalArgumentException(
                    "Unix domain socket path '" + path + "' is too long (maximum " + MAX_UDS_PATH_LENGTH + " bytes).");
        }
        return Path.of(path);
    }

    private static int allocateFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }

    /**
     * Listener for a single channel: accepts connections on a Unix domain socket and pumps
     * bytes bidirectionally to a fresh TCP connection on the loopback target port.
     */
    private static final class ChannelProxy {

        private final String name;
        private final Path udsPath;
        private final int tcpPort;
        private final ServerSocketChannel server;
        private final List<Connection> connections = new ArrayList<>();
        private volatile boolean closed = false;

        ChannelProxy(String name, Path udsPath, int tcpPort) throws IOException {
            this.name = name;
            this.udsPath = udsPath;
            this.tcpPort = tcpPort;

            Files.deleteIfExists(udsPath);
            this.server = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
            this.server.bind(UnixDomainSocketAddress.of(udsPath));
            LOGGER.info("[" + name + "]: ipc proxy bound at '" + udsPath + "', forwarding to tcp://127.0.0.1:" + tcpPort);
        }

        void startAccepting() {
            Thread acceptThread = new Thread(this::acceptLoop, "ipc-proxy-" + name + "-accept");
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        private void acceptLoop() {
            while (!closed) {
                try {
                    SocketChannel udsChannel = server.accept();
                    SocketChannel tcpChannel;
                    try {
                        tcpChannel = SocketChannel.open(new InetSocketAddress("127.0.0.1", tcpPort));
                    } catch (IOException e) {
                        LOGGER.log(Level.WARNING, "[" + name + "]: cannot connect to tcp://127.0.0.1:" + tcpPort, e);
                        closeQuietly(udsChannel);
                        continue;
                    }
                    Connection connection = new Connection(name, udsChannel, tcpChannel);
                    synchronized (connections) {
                        connections.removeIf(Connection::isClosed);
                        connections.add(connection);
                    }
                    connection.startPumping();
                } catch (ClosedChannelException e) {
                    // proxy was closed, exit gracefully
                    return;
                } catch (IOException e) {
                    if (!closed) {
                        LOGGER.log(Level.WARNING, "[" + name + "]: error accepting ipc connection", e);
                    }
                    return;
                }
            }
        }

        void close() {
            closed = true;
            closeQuietly(server);
            synchronized (connections) {
                connections.forEach(Connection::close);
                connections.clear();
            }
            try {
                Files.deleteIfExists(udsPath);
            } catch (IOException e) {
                LOGGER.log(Level.FINE, "[" + name + "]: cannot delete socket file " + udsPath, e);
            }
        }
    }

    /**
     * A pair of channels (unix domain socket and loopback tcp socket) with two pump threads
     * copying bytes in each direction. When one direction reaches end of stream, the write side
     * of the peer is shut down; when both directions finish, both channels are closed.
     */
    private static final class Connection {

        private static final int BUFFER_SIZE = 64 * 1024;

        private final SocketChannel udsChannel;
        private final SocketChannel tcpChannel;
        private final AtomicInteger finishedPumps = new AtomicInteger(0);
        private volatile boolean closed = false;

        private final String name;

        Connection(String name, SocketChannel udsChannel, SocketChannel tcpChannel) {
            this.name = name;
            this.udsChannel = udsChannel;
            this.tcpChannel = tcpChannel;
        }

        void startPumping() {
            startPump(udsChannel, tcpChannel, "ipc-proxy-" + name + "-in");
            startPump(tcpChannel, udsChannel, "ipc-proxy-" + name + "-out");
        }

        private void startPump(SocketChannel src, SocketChannel dst, String threadName) {
            Thread pump = new Thread(() -> {
                ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
                try {
                    while (src.read(buffer) >= 0) {
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            dst.write(buffer);
                        }
                        buffer.clear();
                    }
                    // end of stream: propagate the half-close to the peer
                    try {
                        dst.shutdownOutput();
                    } catch (IOException | UnsupportedOperationException ignored) {
                        // channel might be already closed
                    }
                } catch (IOException e) {
                    // connection broken or closed, terminate both sides
                    close();
                } finally {
                    if (finishedPumps.incrementAndGet() == 2) {
                        close();
                    }
                }
            }, threadName);
            pump.setDaemon(true);
            pump.start();
        }

        boolean isClosed() {
            return closed;
        }

        void close() {
            closed = true;
            closeQuietly(udsChannel);
            closeQuietly(tcpChannel);
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }
}
