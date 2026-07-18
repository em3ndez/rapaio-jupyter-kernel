package org.rapaio.jupyter.kernel.channels;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rapaio.jupyter.kernel.core.ConnectionProperties;

public class IpcProxyTest {

    private Path baseDir;
    private IpcProxy proxy;

    @BeforeEach
    void setUp() throws IOException {
        baseDir = Files.createTempDirectory("ipc-test");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (proxy != null) {
            proxy.close();
        }
        try (var paths = Files.list(baseDir)) {
            for (Path p : paths.toList()) {
                Files.deleteIfExists(p);
            }
        }
        Files.deleteIfExists(baseDir);
    }

    private ConnectionProperties ipcProps() {
        return new ConnectionProperties(1, 2, "ipc", "hmac-sha256", 3, 4,
                baseDir.resolve("kernel-test").toString(), 5, "test-key");
    }

    @Test
    void testStartReturnsTcpProperties() throws IOException {
        ConnectionProperties ipcProps = ipcProps();
        proxy = new IpcProxy(ipcProps);
        ConnectionProperties tcpProps = proxy.start();

        assertEquals("tcp", tcpProps.transport());
        assertEquals("127.0.0.1", tcpProps.ip());
        assertEquals(ipcProps.key(), tcpProps.key());
        assertEquals(ipcProps.signatureScheme(), tcpProps.signatureScheme());

        Set<Integer> ports = Set.of(tcpProps.controlPort(), tcpProps.shellPort(), tcpProps.stdinPort(),
                tcpProps.hbPort(), tcpProps.iopubPort());
        assertEquals(5, ports.size(), "allocated tcp ports must be distinct");
        ports.forEach(port -> assertTrue(port > 0));

        // one unix socket file per channel, named <ip>-<port>
        for (int port : new int[] {1, 2, 3, 4, 5}) {
            assertTrue(Files.exists(Path.of(ipcProps.ip() + "-" + port)),
                    "socket file for port " + port + " must exist");
        }
    }

    @Test
    void testBidirectionalForwarding() throws Exception {
        ConnectionProperties ipcProps = ipcProps();
        proxy = new IpcProxy(ipcProps);
        ConnectionProperties tcpProps = proxy.start();

        // simulate the kernel: an echo server bound on the shell tcp port
        try (ServerSocket kernelSide = new ServerSocket(tcpProps.shellPort(), 5,
                java.net.InetAddress.getLoopbackAddress())) {
            Thread echo = new Thread(() -> {
                try (Socket s = kernelSide.accept()) {
                    s.getInputStream().transferTo(s.getOutputStream());
                } catch (IOException ignored) {
                }
            });
            echo.setDaemon(true);
            echo.start();

            // simulate the jupyter server: connect over the unix domain socket
            UnixDomainSocketAddress address = UnixDomainSocketAddress.of(ipcProps.ip() + "-" + ipcProps.shellPort());
            try (SocketChannel client = SocketChannel.open(address)) {
                byte[] payload = "hello over ipc".getBytes(StandardCharsets.UTF_8);
                ByteBuffer out = ByteBuffer.wrap(payload);
                while (out.hasRemaining()) {
                    client.write(out);
                }

                ByteBuffer in = ByteBuffer.allocate(payload.length);
                while (in.hasRemaining()) {
                    if (client.read(in) < 0) {
                        break;
                    }
                }
                assertEquals("hello over ipc", new String(in.array(), StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void testMultipleConcurrentConnections() throws Exception {
        ConnectionProperties ipcProps = ipcProps();
        proxy = new IpcProxy(ipcProps);
        ConnectionProperties tcpProps = proxy.start();

        int clientCount = 4;
        try (ServerSocket kernelSide = new ServerSocket(tcpProps.iopubPort(), 16,
                java.net.InetAddress.getLoopbackAddress())) {
            CountDownLatch served = new CountDownLatch(clientCount);
            Thread server = new Thread(() -> {
                try {
                    while (true) {
                        Socket s = kernelSide.accept();
                        Thread worker = new Thread(() -> {
                            try (s) {
                                s.getInputStream().transferTo(s.getOutputStream());
                            } catch (IOException ignored) {
                            } finally {
                                served.countDown();
                            }
                        });
                        worker.setDaemon(true);
                        worker.start();
                    }
                } catch (IOException ignored) {
                }
            });
            server.setDaemon(true);
            server.start();

            UnixDomainSocketAddress address = UnixDomainSocketAddress.of(ipcProps.ip() + "-" + ipcProps.iopubPort());
            Set<String> replies = new HashSet<>();
            SocketChannel[] clients = new SocketChannel[clientCount];
            try {
                for (int i = 0; i < clientCount; i++) {
                    clients[i] = SocketChannel.open(address);
                    ByteBuffer out = ByteBuffer.wrap(("client-" + i).getBytes(StandardCharsets.UTF_8));
                    while (out.hasRemaining()) {
                        clients[i].write(out);
                    }
                    clients[i].shutdownOutput();
                }
                for (int i = 0; i < clientCount; i++) {
                    ByteBuffer in = ByteBuffer.allocate(64);
                    while (clients[i].read(in) >= 0) {
                        // read until the proxy propagates end of stream
                    }
                    in.flip();
                    replies.add(StandardCharsets.UTF_8.decode(in).toString());
                }
            } finally {
                for (SocketChannel client : clients) {
                    if (client != null) {
                        client.close();
                    }
                }
            }

            assertTrue(served.await(5, TimeUnit.SECONDS), "all connections must reach the kernel side");
            for (int i = 0; i < clientCount; i++) {
                assertTrue(replies.contains("client-" + i), "reply for client-" + i + " must be received");
            }
        }
    }

    @Test
    void testCloseRemovesSocketFiles() throws IOException {
        ConnectionProperties ipcProps = ipcProps();
        proxy = new IpcProxy(ipcProps);
        proxy.start();

        for (int port : new int[] {1, 2, 3, 4, 5}) {
            assertTrue(Files.exists(Path.of(ipcProps.ip() + "-" + port)));
        }
        proxy.close();
        for (int port : new int[] {1, 2, 3, 4, 5}) {
            assertFalse(Files.exists(Path.of(ipcProps.ip() + "-" + port)),
                    "socket file for port " + port + " must be removed on close");
        }
    }

    @Test
    void testStaleSocketFileIsReplaced() throws IOException {
        ConnectionProperties ipcProps = ipcProps();
        // simulate a stale socket file left over from a previous run
        Files.createFile(Path.of(ipcProps.ip() + "-" + ipcProps.shellPort()));

        proxy = new IpcProxy(ipcProps);
        ConnectionProperties tcpProps = proxy.start();
        assertEquals("tcp", tcpProps.transport());
    }

    @Test
    void testStartTwiceFails() throws IOException {
        proxy = new IpcProxy(ipcProps());
        proxy.start();
        assertThrows(IllegalStateException.class, proxy::start);
    }

    @Test
    void testTooLongPathFails() {
        String longBase = baseDir.resolve("x".repeat(200)).toString();
        ConnectionProperties props = new ConnectionProperties(1, 2, "ipc", "hmac-sha256", 3, 4,
                longBase, 5, "test-key");
        proxy = new IpcProxy(props);
        assertThrows(IllegalArgumentException.class, proxy::start);
    }
}
