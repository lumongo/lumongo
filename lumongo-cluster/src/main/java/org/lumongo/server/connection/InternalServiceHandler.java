package org.lumongo.server.connection;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerPipelineFactory;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;

public class InternalServiceHandler extends InternalService {

	private final static Logger log = Logger.getLogger(InternalServiceHandler.class);

	private final IndexManager indexManager;
	private final ClusterConfig clusterConfig;
	private final LocalNodeConfig localNodeConfig;

	private ServerBootstrap bootstrap;

	public InternalServiceHandler(ClusterConfig clusterConfig, LocalNodeConfig localNodeConfig, IndexManager indexManager) {
		this.clusterConfig = clusterConfig;
		this.localNodeConfig = localNodeConfig;
		this.indexManager = indexManager;

	}

	public void start() {
		int internalServicePort = localNodeConfig.getInternalServicePort();

		PeerInfo internalServerInfo = new PeerInfo(ConnectionHelper.getHostName(), internalServicePort);
		int coreInternalWorkers = clusterConfig.getInternalWorkers();
		int maxInternalWorkers = 1024; // TODO fix this

		RpcServerCallExecutor executor = new ThreadPoolCallExecutor(coreInternalWorkers, maxInternalWorkers, new RenamingThreadFactoryProxy(InternalService.class.getSimpleName() + "-" + localNodeConfig.getHazelcastPort() + "-Rpc", Executors.defaultThreadFactory()));

		DuplexTcpServerPipelineFactory serverFactory = new DuplexTcpServerPipelineFactory(internalServerInfo);
		serverFactory.setRpcServerCallExecutor(executor);

		bootstrap = new ServerBootstrap();
		bootstrap.group(new NioEventLoopGroup(0,new RenamingThreadFactoryProxy(InternalService.class.getSimpleName() + "-" + localNodeConfig.getHazelcastPort() + "-Boss", Executors.defaultThreadFactory())),
				new NioEventLoopGroup(0,new RenamingThreadFactoryProxy(InternalService.class.getSimpleName() + "-" + localNodeConfig.getHazelcastPort() + "-Worker", Executors.defaultThreadFactory()))
				);
		bootstrap.channel(NioServerSocketChannel.class);
		bootstrap.childHandler(serverFactory);

		//TODO think about these options
		bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
		bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);
		bootstrap.childOption(ChannelOption.SO_RCVBUF, 1048576);
		bootstrap.childOption(ChannelOption.SO_SNDBUF, 1048576);
		bootstrap.option(ChannelOption.TCP_NODELAY, true);

		bootstrap.localAddress(internalServicePort);

		serverFactory.setLogger(null);
		serverFactory.registerConnectionEventListener(new StandardConnectionNotifier(log));

		serverFactory.getRpcServiceRegistry().registerService(this);

		bootstrap.bind();
	}

	public void shutdown() {

		int internalShutdownTimeout = clusterConfig.getInternalShutdownTimeout() * 1000;

		Thread internalShutdown = new Thread("InternalServiceShutdown-" + localNodeConfig.getHazelcastPort()) {
			@Override
			public void run() {
				log.info("Stopping internal service");
				bootstrap.shutdown();
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
			InternalIndexResponse r = indexManager.internalIndex(request.getUniqueId(), request.getIndexName(), request.getIndexedDocument());
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
			InternalDeleteResponse r = indexManager.internalDeleteFromIndex(request.getUniqueId(), request.getIndex());
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
