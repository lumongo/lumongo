package org.lumongo.client.pool;

import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.lumongo.client.LumongoRestClient;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.LMMember;
import org.lumongo.util.LumongoThreadFactory;

import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;


public class LumongoConnection {

    private static LumongoThreadFactory bossFactory = new LumongoThreadFactory(LumongoConnection.class.getSimpleName() + "-Boss");
    private static LumongoThreadFactory workerFactory = new LumongoThreadFactory(LumongoConnection.class.getSimpleName() + "-Worker");

    private LMMember member;
    private ExternalService.BlockingInterface service;
    private RpcClient rpcClient;
    private DuplexTcpClientBootstrap bootstrap;
    private final String myHostName;


    private static CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();


    public LumongoConnection(LMMember member) throws IOException {
        this.myHostName = InetAddress.getLocalHost().getCanonicalHostName();
        this.member = member;
    }

    public void open(boolean compressedConnection) throws IOException {

        PeerInfo server = new PeerInfo(member.getServerAddress(), member.getExternalPort());
        PeerInfo client = new PeerInfo(myHostName + "-" + UUID.randomUUID().toString(), 1234);

        System.err.println("INFO: Connecting from <" + client + "> to <" + server + ">");

        ChannelFactory cf = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(bossFactory), Executors.newCachedThreadPool(workerFactory));

        bootstrap = new DuplexTcpClientBootstrap(client, cf);
        bootstrap.setCompression(compressedConnection);
        bootstrap.setRpcLogger(null);
        bootstrap.setOption("connectTimeoutMillis", 10000);
        bootstrap.setOption("connectResponseTimeoutMillis", 10000);
        bootstrap.setOption("receiveBufferSize", 1048576);
        bootstrap.setOption("tcpNoDelay", false);
        shutdownHandler.addResource(bootstrap);

        rpcClient = bootstrap.peerWith(server);

        service = ExternalService.newBlockingStub(rpcClient);

    }

    public LumongoRestClient getRestClient() throws Exception {
        LumongoRestClient lrc = new LumongoRestClient(member.getServerAddress(), member.getRestPort());
        return lrc;
    }

    public RpcController getController() {
        return rpcClient.newRpcController();
    }

    public ExternalService.BlockingInterface getService() {
        return service;
    }

    /**
     * closes the connection to the server if open, calling a method (index, query, ...) will open a new connection
     */
    public void close() {
        try {

            if (rpcClient != null) {
                System.err.println("INFO: Closing connection to " + rpcClient.getPeerInfo());
                rpcClient.close();
            }
        }
        catch (Exception e) {
            System.err.println("ERROR: Exception: " + e);
            e.printStackTrace();
        }
        rpcClient = null;
        try {
            if (bootstrap != null) {
                bootstrap.releaseExternalResources();
            }
        }
        catch (Exception e) {
            System.err.println("ERROR: Exception: " + e);
            e.printStackTrace();
        }
        bootstrap = null;
        service = null;
    }

}
