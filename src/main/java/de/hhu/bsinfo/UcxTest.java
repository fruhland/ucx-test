package de.hhu.bsinfo;

import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class UcxTest {

    private static final short PORT = 2998;

    private final UcpContext context = new UcpContext(new UcpParams()
            .requestStreamFeature()
            .requestTagFeature()
            .setEstimatedNumEps(1));
    private final UcpWorker worker = context.newWorker(new UcpWorkerParams());
    private UcpEndpoint endpoint;

    private final ByteBuffer wireUpBuffer = ByteBuffer.allocateDirect(8);
    private final ByteBuffer messageBuffer = ByteBuffer.allocateDirect(1024);

    public static void main(String[] args) {
        final var test = new UcxTest();
        if (args.length == 0) {
            test.server();
        } else {
            test.client(args[0]);
        }
    }

    private void server() {
        final var listener = worker.newListener(new UcpListenerParams()
                .setSockAddr(new InetSocketAddress(PORT))
                .setConnectionHandler(ucpConnectionRequest -> {
                    System.out.println("Connection request from " + ucpConnectionRequest.getClientAddress());
                    endpoint = worker.newEndpoint(new UcpEndpointParams()
                            .setConnectionRequest(ucpConnectionRequest)
                            .setPeerErrorHandlingMode()
                            .setErrorHandler((ucpEndpoint, i, s) -> {
                                throw new RuntimeException(s);
                            }));
        }));

        try {
            while (endpoint == null) {
                worker.progress();
            }
        } catch (Exception e) {
            System.err.println("Failed to listen for connection requests!");
            e.printStackTrace();
        } finally {
            listener.close();
        }

        try {
            // worker.progressRequest(endpoint.recvStreamNonBlocking(wireUpBuffer, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, null));
            // worker.progressRequest(endpoint.sendStreamNonBlocking(wireUpBuffer, null));
            worker.progressRequest(worker.recvTaggedNonBlocking(wireUpBuffer, null));
            worker.progressRequest(endpoint.sendTaggedNonBlocking(wireUpBuffer, null));
            System.out.println("Connection has been established successfully!");
        } catch (Exception e) {
            System.err.println("Failed to exchange wire up message!");
            e.printStackTrace();
            return;
        }

        try {
            System.out.println("Starting to receive messages!");
            for (int i = 0; i < 1000; i++) {
                // worker.progressRequest(endpoint.recvStreamNonBlocking(messageBuffer, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, null));
                worker.progressRequest(worker.recvTaggedNonBlocking(messageBuffer, null));
            }

            // worker.progressRequest(endpoint.sendStreamNonBlocking(messageBuffer, null));
            worker.progressRequest(endpoint.sendTaggedNonBlocking(messageBuffer, null));
            System.out.println("Finished receiving messages!");
        } catch (Exception e) {
            System.err.println("Failed to receive a message!");
        } finally {
            endpoint.close();
            worker.close();
            context.close();
        }
    }

    private void client(final String remoteHost) {
        final var address = new InetSocketAddress(remoteHost, PORT);
        System.out.println("Connecting to " + address);

        endpoint = worker.newEndpoint(new UcpEndpointParams()
                .setSocketAddress(address).setPeerErrorHandlingMode()
                .setErrorHandler((ucpEndpoint, i, s) -> {
                    throw new RuntimeException(s);
                }));

        try {
            // worker.progressRequest(endpoint.sendStreamNonBlocking(wireUpBuffer, null));
            // worker.progressRequest(endpoint.recvStreamNonBlocking(wireUpBuffer, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, null));
            worker.progressRequest(endpoint.sendTaggedNonBlocking(wireUpBuffer, null));
            worker.progressRequest(worker.recvTaggedNonBlocking(wireUpBuffer, null));
            System.out.println("Connection has been established successfully");
        } catch (Exception e) {
            System.err.println("Failed to exchange wire up message!");
            e.printStackTrace();
            return;
        }

        try {
            System.out.println("Starting to send messages!");
            for (int i = 0; i < 1000; i++) {
                // worker.progressRequest(endpoint.sendStreamNonBlocking(messageBuffer, null));
                worker.progressRequest(endpoint.sendTaggedNonBlocking(messageBuffer, null));
            }

            // worker.progressRequest(endpoint.recvStreamNonBlocking(messageBuffer, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, null));
            worker.progressRequest(worker.recvTaggedNonBlocking(messageBuffer, null));
            System.out.println("Finished sending messages!");
        } catch (Exception e) {
            System.err.println("Failed to send a message!");
        } finally {
            endpoint.close();
            worker.close();
            context.close();
        }
    }
}