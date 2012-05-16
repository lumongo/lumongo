package org.lumongo.server.connection;

import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.lumongo.cluster.message.Lumongo.ClearRequest;
import org.lumongo.cluster.message.Lumongo.ClearResponse;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesRequest;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsRequest;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsResponse;
import org.lumongo.cluster.message.Lumongo.GetTermsRequest;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;
import org.lumongo.cluster.message.Lumongo.InternalDeleteRequest;
import org.lumongo.cluster.message.Lumongo.InternalDeleteResponse;
import org.lumongo.cluster.message.Lumongo.InternalIndexRequest;
import org.lumongo.cluster.message.Lumongo.InternalIndexResponse;
import org.lumongo.cluster.message.Lumongo.InternalQueryResponse;
import org.lumongo.cluster.message.Lumongo.InternalService;
import org.lumongo.cluster.message.Lumongo.OptimizeRequest;
import org.lumongo.cluster.message.Lumongo.OptimizeResponse;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.indexing.IndexManager;
import org.lumongo.util.LumongoThreadFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerBootstrap;

public class InternalServiceHandler extends InternalService {
	
	private final static Logger log = Logger.getLogger(InternalServiceHandler.class);
	
	private LumongoThreadFactory internalRpcFactory;
	private LumongoThreadFactory internalBossFactory;
	private LumongoThreadFactory internalWorkerFactory;
	
	private final IndexManager indexManager;
	private final ClusterConfig clusterConfig;
	private final LocalNodeConfig localNodeConfig;
	
	private DuplexTcpServerBootstrap internalBootstrap;
	
	public InternalServiceHandler(ClusterConfig clusterConfig, LocalNodeConfig localNodeConfig, IndexManager indexManager) {
		this.clusterConfig = clusterConfig;
		this.localNodeConfig = localNodeConfig;
		this.indexManager = indexManager;
		
		internalRpcFactory = new LumongoThreadFactory(InternalService.class.getSimpleName() + "-" + localNodeConfig.getHazelcastPort() + "-Rpc");
		internalBossFactory = new LumongoThreadFactory(InternalService.class.getSimpleName() + "-" + localNodeConfig.getHazelcastPort() + "-Boss");
		internalWorkerFactory = new LumongoThreadFactory(InternalService.class.getSimpleName() + "-" + localNodeConfig.getHazelcastPort() + "-Worker");
	}
	
	public void start() throws ChannelException {
		int internalServicePort = localNodeConfig.getInternalServicePort();
		
		PeerInfo internalServerInfo = new PeerInfo(ConnectionHelper.getHostName(), internalServicePort);
		
		int coreInternalWorkers = clusterConfig.getInternalWorkers();
		int maxInternalWorkers = 1024; // TODO fix this
		
		internalBootstrap = new DuplexTcpServerBootstrap(internalServerInfo, new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(internalBossFactory), Executors.newCachedThreadPool(internalWorkerFactory)), new ThreadPoolCallExecutor(
				coreInternalWorkers, maxInternalWorkers, internalRpcFactory), null);
		internalBootstrap.setOption("sendBufferSize", 1048576);
		internalBootstrap.setOption("receiveBufferSize", 1048576);
		internalBootstrap.setOption("child.receiveBufferSize", 1048576);
		internalBootstrap.setOption("child.sendBufferSize", 1048576);
		internalBootstrap.setOption("tcpNoDelay", false);
		
		internalBootstrap.registerConnectionEventListener(new StandardConnectionNotifier(log));
		
		internalBootstrap.getRpcServiceRegistry().registerService(this);
		
		internalBootstrap.bind();
	}
	
	public void shutdown() {
		
		int internalShutdownTimeout = clusterConfig.getInternalShutdownTimeout() * 1000;
		
		Thread internalShutdown = new Thread("InternalServiceShutdown-" + localNodeConfig.getHazelcastPort()) {
			@Override
			public void run() {
				log.info("Stopping internal service");
				internalBootstrap.releaseExternalResources();
			}
		};
		internalShutdown.start();
		
		try {
			internalShutdown.join(internalShutdownTimeout);
		}
		catch (Exception e) {
			
		}
		
		if (internalShutdown.isAlive()) {
			log.info("Failed to stop internal service within " + internalShutdownTimeout + "ms");
		}
	}
	
	@Override
	public void queryInternal(RpcController controller, QueryRequest request, RpcCallback<InternalQueryResponse> done) {
		try {
			InternalQueryResponse r = indexManager.internalQuery(request);
			done.run(r);
		}
		catch (Exception e) {
			log.error("Failed to run internal query: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			controller.setFailed(e.getMessage());
			done.run(null);
		}
	}
	
	@Override
	public void indexInternal(RpcController controller, InternalIndexRequest request, RpcCallback<InternalIndexResponse> done) {
		try {
			InternalIndexResponse r = indexManager.internalIndex(request.getUniqueId(), request.getIndexedDocumentList());
			done.run(r);
		}
		catch (Exception e) {
			log.error("Failed to run internal index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			controller.setFailed(e.getMessage());
			done.run(null);
		}
	}
	
	@Override
	public void deleteInternal(RpcController controller, InternalDeleteRequest request, RpcCallback<InternalDeleteResponse> done) {
		try {
			InternalDeleteResponse r = indexManager.internalDeleteFromIndex(request.getUniqueId(), request.getIndexesList());
			done.run(r);
		}
		catch (Exception e) {
			log.error("Failed to run internal delete: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			controller.setFailed(e.getMessage());
			done.run(null);
		}
	}
	
	@Override
	public void getNumberOfDocsInternal(RpcController controller, GetNumberOfDocsRequest request, RpcCallback<GetNumberOfDocsResponse> done) {
		try {
			GetNumberOfDocsResponse r = indexManager.getNumberOfDocsInternal(request);
			done.run(r);
		}
		catch (Exception e) {
			log.error("Failed to run get number of docs: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			controller.setFailed(e.getMessage());
			done.run(null);
		}
	}
	
	@Override
	public void clear(RpcController controller, ClearRequest request, RpcCallback<ClearResponse> done) {
		try {
			ClearResponse r = indexManager.clearInternal(request);
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
			OptimizeResponse r = indexManager.optimizeInternal(request);
			done.run(r);
		}
		catch (Exception e) {
			log.error("Failed to optimized index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			controller.setFailed(e.getMessage());
			done.run(null);
		}
	}
	
	@Override
	public void getFieldNames(RpcController controller, GetFieldNamesRequest request, RpcCallback<GetFieldNamesResponse> done) {
		try {
			GetFieldNamesResponse r = indexManager.getFieldNamesInternal(request);
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
			GetTermsResponse r = indexManager.getTermsInternal(request);
			done.run(r);
		}
		catch (Exception e) {
			log.error("Failed to get terms: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			controller.setFailed(e.getMessage());
			done.run(null);
		}
	}
	
}
