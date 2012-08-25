package org.lumongo.server.connection;

import java.net.UnknownHostException;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.lumongo.cluster.message.Lumongo.ClearRequest;
import org.lumongo.cluster.message.Lumongo.ClearResponse;
import org.lumongo.cluster.message.Lumongo.DeleteRequest;
import org.lumongo.cluster.message.Lumongo.DeleteResponse;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.FetchRequest;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesRequest;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;
import org.lumongo.cluster.message.Lumongo.GetIndexesRequest;
import org.lumongo.cluster.message.Lumongo.GetIndexesResponse;
import org.lumongo.cluster.message.Lumongo.GetMembersRequest;
import org.lumongo.cluster.message.Lumongo.GetMembersResponse;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsRequest;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsResponse;
import org.lumongo.cluster.message.Lumongo.GetTermsRequest;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;
import org.lumongo.cluster.message.Lumongo.GroupFetchRequest;
import org.lumongo.cluster.message.Lumongo.GroupFetchResponse;
import org.lumongo.cluster.message.Lumongo.IndexCreateRequest;
import org.lumongo.cluster.message.Lumongo.IndexCreateResponse;
import org.lumongo.cluster.message.Lumongo.IndexDeleteRequest;
import org.lumongo.cluster.message.Lumongo.IndexDeleteResponse;
import org.lumongo.cluster.message.Lumongo.IndexSettingsRequest;
import org.lumongo.cluster.message.Lumongo.IndexSettingsResponse;
import org.lumongo.cluster.message.Lumongo.OptimizeRequest;
import org.lumongo.cluster.message.Lumongo.OptimizeResponse;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.StoreRequest;
import org.lumongo.cluster.message.Lumongo.StoreResponse;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.indexing.IndexManager;
import org.lumongo.util.LumongoThreadFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogger;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;

public class ExternalServiceHandler extends ExternalService {
    private final static Logger log = Logger.getLogger(ExternalServiceHandler.class);

    private final LumongoThreadFactory externalRpcFactory;
    private final LumongoThreadFactory externalBossFactory;
    private final LumongoThreadFactory externalWorkerFactory;

    private final IndexManager indexManger;
    private final ClusterConfig clusterConfig;
    private final LocalNodeConfig localNodeConfig;

    private DuplexTcpServerBootstrap externalBootstrap;

    public ExternalServiceHandler(ClusterConfig clusterConfig, LocalNodeConfig localNodeConfig, IndexManager indexManger) throws UnknownHostException {
        this.clusterConfig = clusterConfig;
        this.localNodeConfig = localNodeConfig;
        this.indexManger = indexManger;
        externalRpcFactory = new LumongoThreadFactory(ExternalService.class.getSimpleName() + "-" + localNodeConfig.getHazelcastPort() + "-Rpc");
        externalBossFactory = new LumongoThreadFactory(ExternalService.class.getSimpleName() + "-" + localNodeConfig.getHazelcastPort() + "-Boss");
        externalWorkerFactory = new LumongoThreadFactory(ExternalService.class.getSimpleName() + "-" + localNodeConfig.getHazelcastPort() + "-Worker");
    }

    public void start() throws ChannelException {
        int externalServicePort = localNodeConfig.getExternalServicePort();
        PeerInfo externalServerInfo = new PeerInfo(ConnectionHelper.getHostName(), externalServicePort);
        RpcLogger externalLogger = null;

        NioServerSocketChannelFactory externalSocketChannelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(externalBossFactory),
                Executors.newCachedThreadPool(externalWorkerFactory));

        int externalWorkers = clusterConfig.getExternalWorkers();

        ThreadPoolCallExecutor externalRpcServerCallExecutor = new ThreadPoolCallExecutor(externalWorkers, externalWorkers, externalRpcFactory);
        externalBootstrap = new DuplexTcpServerBootstrap(externalServerInfo, externalSocketChannelFactory, externalRpcServerCallExecutor, externalLogger);

        externalBootstrap.setOption("sendBufferSize", 1048576);
        externalBootstrap.setOption("receiveBufferSize", 1048576);
        externalBootstrap.setOption("child.receiveBufferSize", 1048576);
        externalBootstrap.setOption("child.sendBufferSize", 1048576);
        externalBootstrap.setOption("tcpNoDelay", false);

        externalBootstrap.registerConnectionEventListener(new StandardConnectionNotifier(log));
        externalBootstrap.getRpcServiceRegistry().registerService(this);

        @SuppressWarnings("unused")
        Channel serverChannel = externalBootstrap.bind();

    }

    public void shutdown() {

        int externalShutdownTimeout = clusterConfig.getExternalShutdownTimeout() * 1000;

        Thread externalShutdown = new Thread("ExternalServiceShutdown-" + localNodeConfig.getHazelcastPort()) {
            @Override
            public void run() {
                log.info("Stopping external service");
                externalBootstrap.releaseExternalResources();
            }
        };
        externalShutdown.start();

        try {
            externalShutdown.join(externalShutdownTimeout);
        }
        catch (Exception e) {

        }

        if (externalShutdown.isAlive()) {
            log.info("Failed to stop external service within " + externalShutdownTimeout + "ms");
        }

    }

    @Override
    public void query(RpcController controller, QueryRequest request, RpcCallback<QueryResponse> done) {
        try {
            QueryResponse qr = indexManger.query(request);
            done.run(qr);
        }
        catch (Exception e) {
            log.error("Failed to run query: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

    @Override
    public void store(RpcController controller, StoreRequest request, RpcCallback<StoreResponse> done) {
        try {
            StoreResponse sr = indexManger.storeDocument(request);
            done.run(sr);
        }
        catch (Exception e) {
            log.error("Failed to store: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            controller.startCancel();
            done.run(null);
        }
    }

    @Override
    public void delete(RpcController controller, DeleteRequest request, RpcCallback<DeleteResponse> done) {
        try {
            DeleteResponse dr = indexManger.deleteDocument(request);
            done.run(dr);
        }
        catch (Exception e) {
            log.error("Failed to delete: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

    @Override
    public void fetch(RpcController controller, FetchRequest request, RpcCallback<FetchResponse> done) {
        try {
            FetchResponse fr = indexManger.fetch(request);
            done.run(fr);
        }
        catch (Exception e) {
            log.error("Failed to fetch: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

    @Override
    public void createIndex(RpcController controller, IndexCreateRequest request, RpcCallback<IndexCreateResponse> done) {
        try {
            IndexCreateResponse r = indexManger.createIndex(request);
            done.run(r);
        }
        catch (Exception e) {
            log.error("Failed to create index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }

    }

    @Override
    public void changeIndex(RpcController controller, IndexSettingsRequest request, RpcCallback<IndexSettingsResponse> done) {
        try {
            IndexSettingsResponse r = indexManger.updateIndex(request.getIndexName(), request.getIndexSettings());
            done.run(r);
        }
        catch (Exception e) {
            log.error("Failed to change index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }

    }

    @Override
    public void deleteIndex(RpcController controller, IndexDeleteRequest request, RpcCallback<IndexDeleteResponse> done) {
        try {
            IndexDeleteResponse r = indexManger.deleteIndex(request);
            done.run(r);
        }
        catch (Exception e) {
            log.error("Failed to delete index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

    @Override
    public void getIndexes(RpcController controller, GetIndexesRequest request, RpcCallback<GetIndexesResponse> done) {
        try {
            GetIndexesResponse r = indexManger.getIndexes(request);
            done.run(r);
        }
        catch (Exception e) {
            log.error("Failed to get indexes: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

    @Override
    public void getNumberOfDocs(RpcController controller, GetNumberOfDocsRequest request, RpcCallback<GetNumberOfDocsResponse> done) {
        try {
            GetNumberOfDocsResponse r = indexManger.getNumberOfDocs(request);
            done.run(r);
        }
        catch (Exception e) {
            log.error("Failed to get number of docs: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

    @Override
    public void clear(RpcController controller, ClearRequest request, RpcCallback<ClearResponse> done) {
        try {
            ClearResponse r = indexManger.clearIndex(request);
            done.run(r);
        }
        catch (Exception e) {
            log.error("Failed to clear index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

    @Override
    public void optimize(RpcController controller, OptimizeRequest request, RpcCallback<OptimizeResponse> done) {
        try {
            OptimizeResponse r = indexManger.optimize(request);
            done.run(r);
        }
        catch (Exception e) {
            log.error("Failed to optimize index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

    @Override
    public void getFieldNames(RpcController controller, GetFieldNamesRequest request, RpcCallback<GetFieldNamesResponse> done) {
        try {
            GetFieldNamesResponse r = indexManger.getFieldNames(request);
            done.run(r);
        }
        catch (Exception e) {
            log.error("Failed to get field names: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

    @Override
    public void getTerms(RpcController controller, GetTermsRequest request, RpcCallback<GetTermsResponse> done) {
        try {
            GetTermsResponse r = indexManger.getTerms(request);
            done.run(r);
        }
        catch (Exception e) {
            log.error("Failed to get terms: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

    @Override
    public void getMembers(RpcController controller, GetMembersRequest request, RpcCallback<GetMembersResponse> done) {
        try {
            GetMembersResponse r = indexManger.getMembers(request);
            done.run(r);
        }
        catch (Exception e) {
            log.error("Failed to get members: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

    @Override
    public void groupFetch(RpcController controller, GroupFetchRequest request, RpcCallback<GroupFetchResponse> done) {
        try {
            GroupFetchResponse.Builder gfrb = GroupFetchResponse.newBuilder();
            for (FetchRequest fr : request.getFetchRequestList()) {
                FetchResponse res = indexManger.fetch(fr);
                gfrb.addFetchResponse(res);
            }
            done.run(gfrb.build());
        }
        catch (Exception e) {
            log.error("Failed to group fetch: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
            controller.setFailed(e.getMessage());
            done.run(null);
        }
    }

}
