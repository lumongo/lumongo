package org.lumongo.server.connection;

import com.hazelcast.core.Member;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import org.apache.log4j.Logger;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.*;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.Nodes;
import org.lumongo.util.ClusterHelper;
import org.lumongo.util.cache.MetaKeys;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InternalClient {
	private final static Logger log = Logger.getLogger(InternalClient.class);

	private ConcurrentHashMap<Member, InternalRpcConnectionPool> internalConnectionPoolMap;
	private ConcurrentHashMap<Member, ReadWriteLock> internalConnectionLockMap;

	private ClusterConfig clusterConfig;
	private ClusterHelper clusterHelper;

	public InternalClient(ClusterHelper clusterHelper, ClusterConfig clusterConfig) {
		this.clusterConfig = clusterConfig;
		this.clusterHelper = clusterHelper;
		this.internalConnectionPoolMap = new ConcurrentHashMap<>();
		this.internalConnectionLockMap = new ConcurrentHashMap<>();

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
				Nodes nodes = clusterHelper.getNodes();

				LocalNodeConfig localNodeConfig = nodes.find(m);

				int internalServicePort = localNodeConfig.getInternalServicePort();

				log.info("Adding connection pool for member <" + m + "> using port <" + internalServicePort + ">");

				int maxConnections = clusterConfig.getMaxInternalClientConnections();

				internalConnectionPoolMap.put(m, new InternalRpcConnectionPool(m.getSocketAddress().getHostName(), internalServicePort, maxConnections));
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

			InternalQueryResponse response = rpcConnection.getService().query(request);

			returnInternalBlockingConnection(m, rpcConnection, true);
			return response;
		}
		catch (StatusRuntimeException e) {
			Metadata trailers = e.getTrailers();
			if (trailers.containsKey(MetaKeys.ERROR_KEY)) {
				throw new Exception(trailers.get(MetaKeys.ERROR_KEY));
			}
			else {
				throw e;
			}
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
			StoreResponse response = rpcConnection.getService().store(request);

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (StatusRuntimeException e) {
			Metadata trailers = e.getTrailers();
			if (trailers.containsKey(MetaKeys.ERROR_KEY)) {
				throw new Exception(trailers.get(MetaKeys.ERROR_KEY));
			}
			else {
				throw e;
			}
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
			DeleteResponse response = rpcConnection.getService().delete(request);

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (StatusRuntimeException e) {
			Metadata trailers = e.getTrailers();
			if (trailers.containsKey(MetaKeys.ERROR_KEY)) {
				throw new Exception(trailers.get(MetaKeys.ERROR_KEY));
			}
			else {
				throw e;
			}
		}
		catch (Exception e) {
			returnInternalBlockingConnection(m, rpcConnection, false);
			throw e;
		}
		finally {
			lock.readLock().unlock();
		}

	}

	public Lumongo.FetchResponse executeFetch(Member m, Lumongo.FetchRequest request) throws Exception {

		ReadWriteLock lock = getLockForMember(m);
		lock.readLock().lock();

		InternalRpcConnection rpcConnection = null;
		try {
			rpcConnection = getInternalRpcConnection(m);
			Lumongo.FetchResponse response = rpcConnection.getService().fetch(request);

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (StatusRuntimeException e) {
			Metadata trailers = e.getTrailers();
			if (trailers.containsKey(MetaKeys.ERROR_KEY)) {
				throw new Exception(trailers.get(MetaKeys.ERROR_KEY));
			}
			else {
				throw e;
			}
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
			GetNumberOfDocsResponse response = rpcConnection.getService().getNumberOfDocs(request);

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

			OptimizeResponse response = rpcConnection.getService().optimize(request);

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (StatusRuntimeException e) {
			Metadata trailers = e.getTrailers();
			if (trailers.containsKey(MetaKeys.ERROR_KEY)) {
				throw new Exception(trailers.get(MetaKeys.ERROR_KEY));
			}
			else {
				throw e;
			}
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

			GetFieldNamesResponse response = rpcConnection.getService().getFieldNames(request);

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (StatusRuntimeException e) {
			Metadata trailers = e.getTrailers();
			if (trailers.containsKey(MetaKeys.ERROR_KEY)) {
				throw new Exception(trailers.get(MetaKeys.ERROR_KEY));
			}
			else {
				throw e;
			}
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

			ClearResponse response = rpcConnection.getService().clear(request);

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

	public Lumongo.GetTermsResponseInternal getTerms(Member m, GetTermsRequest request) throws Exception {
		ReadWriteLock lock = getLockForMember(m);
		lock.readLock().lock();

		InternalRpcConnection rpcConnection = null;
		try {
			rpcConnection = getInternalRpcConnection(m);

			Lumongo.GetTermsResponseInternal response = rpcConnection.getService().getTerms(request);

			returnInternalBlockingConnection(m, rpcConnection, true);

			return response;
		}
		catch (StatusRuntimeException e) {
			Metadata trailers = e.getTrailers();
			if (trailers.containsKey(MetaKeys.ERROR_KEY)) {
				throw new Exception(trailers.get(MetaKeys.ERROR_KEY));
			}
			else {
				throw e;
			}
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
