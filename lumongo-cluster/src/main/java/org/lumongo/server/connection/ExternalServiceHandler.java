package org.lumongo.server.connection;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.NonInterruptingThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerPipelineFactory;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.BasicBSONObject;
import org.lumongo.cluster.message.Lumongo.BatchDeleteRequest;
import org.lumongo.cluster.message.Lumongo.BatchDeleteResponse;
import org.lumongo.cluster.message.Lumongo.BatchFetchRequest;
import org.lumongo.cluster.message.Lumongo.BatchFetchResponse;
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
import org.lumongo.server.index.LumongoIndexManager;

import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExternalServiceHandler extends ExternalService {
	private final static Logger log = Logger.getLogger(ExternalServiceHandler.class);
	
	private final LumongoIndexManager indexManger;
	private final ClusterConfig clusterConfig;
	private final LocalNodeConfig localNodeConfig;
	
	private ServerBootstrap bootstrap;
	
	public ExternalServiceHandler(ClusterConfig clusterConfig, LocalNodeConfig localNodeConfig, LumongoIndexManager indexManger) throws UnknownHostException {
		this.clusterConfig = clusterConfig;
		this.localNodeConfig = localNodeConfig;
		this.indexManger = indexManger;
		
	}
	
	public void start() {
		int externalServicePort = localNodeConfig.getExternalServicePort();
		PeerInfo externalServerInfo = new PeerInfo(ConnectionHelper.getHostName(), externalServicePort);
		
		int externalWorkers = clusterConfig.getExternalWorkers();
		
		RpcServerCallExecutor executor = new NonInterruptingThreadPoolCallExecutor(externalWorkers, externalWorkers, new RenamingThreadFactoryProxy(
						ExternalService.class.getSimpleName() + "-" + localNodeConfig.getHazelcastPort() + "-Rpc", Executors.defaultThreadFactory()));
		
		DuplexTcpServerPipelineFactory serverFactory = new DuplexTcpServerPipelineFactory(externalServerInfo);
		serverFactory.setRpcServerCallExecutor(executor);
		
		bootstrap = new ServerBootstrap();
		bootstrap.group(new NioEventLoopGroup(0, new RenamingThreadFactoryProxy(ExternalService.class.getSimpleName() + "-"
										+ localNodeConfig.getHazelcastPort() + "-Boss", Executors.defaultThreadFactory())),
						new NioEventLoopGroup(0, new RenamingThreadFactoryProxy(ExternalService.class.getSimpleName() + "-"
										+ localNodeConfig.getHazelcastPort() + "-Worker", Executors.defaultThreadFactory())));
		bootstrap.channel(NioServerSocketChannel.class);
		bootstrap.childHandler(serverFactory);
		
		//TODO think about these options
		bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
		bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);
		bootstrap.childOption(ChannelOption.SO_RCVBUF, 1048576);
		bootstrap.childOption(ChannelOption.SO_SNDBUF, 1048576);
		bootstrap.option(ChannelOption.TCP_NODELAY, true);
		
		bootstrap.localAddress(externalServicePort);
		
		serverFactory.setLogger(null);
		serverFactory.registerConnectionEventListener(new StandardConnectionNotifier(log));
		
		serverFactory.getRpcServiceRegistry().registerService(this);
		
		bootstrap.bind();
		
	}
	
	public void shutdown() {
		
		log.info("Starting external service shutdown");
		bootstrap.group().shutdownGracefully(1, clusterConfig.getExternalShutdownTimeout(), TimeUnit.SECONDS);
		
		try {
			bootstrap.group().terminationFuture().sync();
		}
		catch (Exception e) {
			log.info("Failed to stop external service within " + clusterConfig.getExternalShutdownTimeout() + "s" + e);
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
			log.error("Failed to store: <" + request.getUniqueId() + "> in index <" + request.getIndexName() + ">: " + e.getClass().getSimpleName() + ": ", e);
			if (request.hasResultDocument()) {
				try {
					BasicBSONObject document = (BasicBSONObject) BSON.decode(request.getResultDocument().getDocument().toByteArray());
					log.error(document.toString());
				}
				catch (Exception e2) {

				}
			}

			controller.setFailed(e.getMessage());
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
	public void batchFetch(RpcController controller, BatchFetchRequest request, RpcCallback<BatchFetchResponse> done) {
		try {
			BatchFetchResponse.Builder gfrb = BatchFetchResponse.newBuilder();
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
	
	@Override
	public void batchDelete(RpcController controller, BatchDeleteRequest request, RpcCallback<BatchDeleteResponse> done) {
		try {
			
			for (DeleteRequest dr : request.getRequestList()) {
				@SuppressWarnings("unused")
				DeleteResponse res = indexManger.deleteDocument(dr);
			}
			done.run(BatchDeleteResponse.newBuilder().build());
		}
		catch (Exception e) {
			log.error("Failed to batch delete: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			controller.setFailed(e.getMessage());
			done.run(null);
		}
	}
	
}
