package org.lumongo.server.connection;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.lumongo.cluster.message.Lumongo.ClearRequest;
import org.lumongo.cluster.message.Lumongo.ClearResponse;
import org.lumongo.cluster.message.Lumongo.DeleteRequest;
import org.lumongo.cluster.message.Lumongo.DeleteResponse;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesRequest;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsRequest;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsResponse;
import org.lumongo.cluster.message.Lumongo.GetTermsRequest;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;
import org.lumongo.cluster.message.Lumongo.InternalQueryResponse;
import org.lumongo.cluster.message.Lumongo.OptimizeRequest;
import org.lumongo.cluster.message.Lumongo.OptimizeResponse;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.StoreRequest;
import org.lumongo.cluster.message.Lumongo.StoreResponse;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.server.config.Nodes;
import org.lumongo.util.ClusterHelper;

import com.google.protobuf.RpcController;
import com.hazelcast.core.Member;

public class InternalClient {
	private final static Logger log = Logger.getLogger(InternalClient.class);

	private ConcurrentHashMap<Member, InternalRpcConnectionPool> internalConnectionPoolMap;
	private ConcurrentHashMap<Member, ReadWriteLock> internalConnectionLockMap;

	private ClusterConfig clusterConfig;
	private MongoConfig mongoConfig;

	public InternalClient(MongoConfig mongoConfig, ClusterConfig clusterConfig) {
		this.clusterConfig = clusterConfig;
		this.mongoConfig = mongoConfig;
		this.internalConnectionPoolMap = new ConcurrentHashMap<Member, InternalRpcConnectionPool>();
		this.internalConnectionLockMap = new ConcurrentHashMap<Member, ReadWriteLock>();

	}

	public void close() {

		for (Member m : internalConnectionPoolMap.keySet()) {
			try {
				internalConnectionPoolMap.get(m).close();
			}
			catch (Exception e) {
				log.info(e.getClass().getSimpleName() + ": ", e);
			}

		}
	}

	public void addMember(Member m) throws Exception {
		ReadWriteLock lock = getLockForMember(m);
		lock.writeLock().lock();
		try {

			if (!internalConnectionPoolMap.containsKey(m)) {
				Nodes nodes = ClusterHelper.getNodes(mongoConfig);

				LocalNodeConfig localNodeConfig = nodes.find(m);

				int internalServicePort = localNodeConfig.getInternalServicePort();

				log.info("Adding connection pool for member <" + m + "> using port <" + internalServicePort + ">");

				int maxConnections = clusterConfig.getMaxInternalClientConnections();

				internalConnectionPoolMap.put(m, new InternalRpcConnectionPool(m.getInetSocketAddress().getHostName(), internalServicePort, maxConnections));
			}
			else {
				log.info("Already loaded connection for member <" + m + ">");
			}
		}
		finally {
			lock.writeLock().unlock();
		}
	}

	public void removeMember(Member m) {

		ReadWriteLock lock = getLockForMember(m);
		lock.writeLock().lock();
		try {
			log.info("Removing connection pool for member <" + m + ">");
			InternalRpcConnectionPool connectionPool = internalConnectionPoolMap.remove(m);
			try {
				connectionPool.close();
			}
			catch (Exception e) {
				// exception is deprecated by BaseObjectPool close
			}
		}
		finally {
			lock.writeLock().unlock();
		}

	}

	private ReadWriteLock getLockForMember(Member m) {

		ReadWriteLock lock = internalConnectionLockMap.get(m);
		if (lock == null) {
			internalConnectionLockMap.putIfAbsent(m, new ReentrantReadWriteLock());
			lock = internalConnectionLockMap.get(m);
		}
		return lock;
	}

	private InternalRpcConnection getInternalRpcConnection(Member m) throws Exception {
		InternalRpcConnectionPool connectionPool = internalConnectionPoolMap.get(m);
		if (connectionPool != null) {
			return connectionPool.borrowObject();
		}
		throw new Exception("Cannot get connection: Member <" + m + "> not loaded");

	}

	private void returnInternalBlockingConnection(Member m, InternalRpcConnection rpcConnection, boolean valid) {
		InternalRpcConnectionPool connectionPool = internalConnectionPoolMap.get(m);
		if (connectionPool != null) {
			try {
				if (valid) {
					connectionPool.returnObject(rpcConnection);
				}
				else {
					connectionPool.invalidateObject(rpcConnection);
				}
			}
			catch (Exception e) {
				log.error("Failed to return blocking connection to member <" + m + "> pool: ", e);
			}
		}
		else {
			log.error("Failed to return blocking connection to member <" + m + "> pool. Pool does not exist.");
			log.error("Current pool members <" + internalConnectionPoolMap.keySet() + ">");
			if (rpcConnection != null) {
				rpcConnection.close();
			}
		}
	}

	public InternalQueryResponse executeQuery(Member m, QueryRequest request) throws Exception {

		ReadWriteLock lock = getLockForMember(m);
		lock.readLock().lock();

		InternalRpcConnection rpcConnection = null;
		try {
			rpcConnection = getInternalRpcConnection(m);
			RpcController controller = rpcConnection.getClientRPCController();
			InternalQueryResponse response = rpcConnection.getService().query(controller, request);
			if (controller.failed()) {
				throw new Exception(m + ":" + controller.errorText());
			}

			returnInternalBlockingConnection(m, rpcConnection, true);
			return response;
		}
		catch (Exception e) {
			returnInternalBlockingConnection(m, rpcConnection, false);
			throw e;
		}
		finally {
			lock.readLock().unlock();
		}

	}

	public StoreResponse executeStore(Member m, StoreRequest request) throws Exception {

		ReadWriteLock lock = getLockForMember(m);
		lock.readLock().lock();

		InternalRpcConnection rpcConnection = null;
		try {
			rpcConnection = getInternalRpcConnection(m);
			RpcController controller = rpcConnection.getClientRPCController();
			StoreResponse response = rpcConnection.getService().store(controller, request);
			if (controller.failed()) {
				throw new Exception(m + ":" + controller.errorText());
			}

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (Exception e) {
			returnInternalBlockingConnection(m, rpcConnection, false);
			throw e;
		}
		finally {
			lock.readLock().unlock();
		}

	}

	public DeleteResponse executeDelete(Member m, DeleteRequest request) throws Exception {

		ReadWriteLock lock = getLockForMember(m);
		lock.readLock().lock();

		InternalRpcConnection rpcConnection = null;
		try {
			rpcConnection = getInternalRpcConnection(m);
			RpcController controller = rpcConnection.getClientRPCController();
			DeleteResponse response = rpcConnection.getService().delete(controller, request);
			if (controller.failed()) {
				throw new Exception(m + ":" + controller.errorText());
			}

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (Exception e) {
			returnInternalBlockingConnection(m, rpcConnection, false);
			throw e;
		}
		finally {
			lock.readLock().unlock();
		}

	}

	public GetNumberOfDocsResponse getNumberOfDocs(Member m, GetNumberOfDocsRequest request) throws Exception {
		ReadWriteLock lock = getLockForMember(m);
		lock.readLock().lock();

		InternalRpcConnection rpcConnection = null;
		try {
			rpcConnection = getInternalRpcConnection(m);
			RpcController controller = rpcConnection.getClientRPCController();
			GetNumberOfDocsResponse response = rpcConnection.getService().getNumberOfDocs(controller, request);
			if (controller.failed()) {
				throw new Exception(m + ":" + controller.errorText());
			}

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (Exception e) {
			returnInternalBlockingConnection(m, rpcConnection, false);
			throw e;
		}
		finally {
			lock.readLock().unlock();
		}
	}

	public OptimizeResponse optimize(Member m, OptimizeRequest request) throws Exception {
		ReadWriteLock lock = getLockForMember(m);
		lock.readLock().lock();

		InternalRpcConnection rpcConnection = null;
		try {
			rpcConnection = getInternalRpcConnection(m);
			RpcController controller = rpcConnection.getClientRPCController();
			OptimizeResponse response = rpcConnection.getService().optimize(controller, request);
			if (controller.failed()) {
				throw new Exception(m + ":" + controller.errorText());
			}

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (Exception e) {
			returnInternalBlockingConnection(m, rpcConnection, false);
			throw e;
		}
		finally {
			lock.readLock().unlock();
		}
	}

	public GetFieldNamesResponse getFieldNames(Member m, GetFieldNamesRequest request) throws Exception {
		ReadWriteLock lock = getLockForMember(m);
		lock.readLock().lock();

		InternalRpcConnection rpcConnection = null;
		try {
			rpcConnection = getInternalRpcConnection(m);
			RpcController controller = rpcConnection.getClientRPCController();
			GetFieldNamesResponse response = rpcConnection.getService().getFieldNames(controller, request);
			if (controller.failed()) {
				throw new Exception(m + ":" + controller.errorText());
			}

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (Exception e) {
			returnInternalBlockingConnection(m, rpcConnection, false);
			throw e;
		}
		finally {
			lock.readLock().unlock();
		}
	}

	public ClearResponse clear(Member m, ClearRequest request) throws Exception {
		ReadWriteLock lock = getLockForMember(m);
		lock.readLock().lock();

		InternalRpcConnection rpcConnection = null;
		try {
			rpcConnection = getInternalRpcConnection(m);
			RpcController controller = rpcConnection.getClientRPCController();
			ClearResponse response = rpcConnection.getService().clear(controller, request);
			if (controller.failed()) {
				throw new Exception(m + ":" + controller.errorText());
			}

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (Exception e) {
			returnInternalBlockingConnection(m, rpcConnection, false);
			throw e;
		}
		finally {
			lock.readLock().unlock();
		}
	}

	public GetTermsResponse getTerms(Member m, GetTermsRequest request) throws Exception {
		ReadWriteLock lock = getLockForMember(m);
		lock.readLock().lock();

		InternalRpcConnection rpcConnection = null;
		try {
			rpcConnection = getInternalRpcConnection(m);
			RpcController controller = rpcConnection.getClientRPCController();
			GetTermsResponse response = rpcConnection.getService().getTerms(controller, request);
			if (controller.failed()) {
				throw new Exception(m + ":" + controller.errorText());
			}

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (Exception e) {
			returnInternalBlockingConnection(m, rpcConnection, false);
			throw e;
		}
		finally {
			lock.readLock().unlock();
		}
	}

}
