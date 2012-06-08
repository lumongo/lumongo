package org.lumongo.server.indexing;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.search.Query;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.ClearRequest;
import org.lumongo.cluster.message.Lumongo.ClearResponse;
import org.lumongo.cluster.message.Lumongo.DeleteRequest;
import org.lumongo.cluster.message.Lumongo.DeleteResponse;
import org.lumongo.cluster.message.Lumongo.FetchRequest;
import org.lumongo.cluster.message.Lumongo.FetchRequest.FetchType;
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
import org.lumongo.cluster.message.Lumongo.IndexSegmentResponse;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.cluster.message.Lumongo.IndexSettingsResponse;
import org.lumongo.cluster.message.Lumongo.InternalDeleteRequest;
import org.lumongo.cluster.message.Lumongo.InternalDeleteResponse;
import org.lumongo.cluster.message.Lumongo.InternalIndexRequest;
import org.lumongo.cluster.message.Lumongo.InternalIndexResponse;
import org.lumongo.cluster.message.Lumongo.InternalQueryResponse;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMMember;
import org.lumongo.cluster.message.Lumongo.OptimizeRequest;
import org.lumongo.cluster.message.Lumongo.OptimizeResponse;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.cluster.message.Lumongo.SegmentCountResponse;
import org.lumongo.cluster.message.Lumongo.StoreRequest;
import org.lumongo.cluster.message.Lumongo.StoreResponse;
import org.lumongo.cluster.message.Lumongo.Term;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.IndexConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.server.config.Nodes;
import org.lumongo.server.connection.InternalClient;
import org.lumongo.server.connection.SocketRequestFederator;
import org.lumongo.server.exceptions.IndexDoesNotExist;
import org.lumongo.server.exceptions.InvalidIndexConfig;
import org.lumongo.server.exceptions.SegmentDoesNotExist;
import org.lumongo.server.hazelcast.HazelcastManager;
import org.lumongo.server.hazelcast.ReloadIndexSettingsTask;
import org.lumongo.server.hazelcast.UnloadIndexTask;
import org.lumongo.server.searching.QueryCombiner;
import org.lumongo.storage.constants.MongoConstants;
import org.lumongo.storage.rawfiles.MongoDocumentStorage;
import org.lumongo.util.ClusterHelper;
import org.lumongo.util.LumongoThreadFactory;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Member;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

public class IndexManager {
	private final static Logger log = Logger.getLogger(IndexManager.class);
	
	private Mongo mongo;
	private MongoDocumentStorage documentStorage;
	
	private final ReadWriteLock globalLock;
	
	private final ConcurrentHashMap<String, Index> indexMap;
	private final InternalClient internalClient;
	
	private final ExecutorService pool;
	
	private HazelcastManager hazelcastManager;
	
	private MongoConfig mongoConfig;
	private ClusterConfig clusterConfig;
	
	public IndexManager(MongoConfig mongoConfig, ClusterConfig clusterConfig) {
		this.globalLock = new ReentrantReadWriteLock(true);
		
		this.mongoConfig = mongoConfig;
		this.clusterConfig = clusterConfig;
		
		this.indexMap = new ConcurrentHashMap<String, Index>();
		this.internalClient = new InternalClient(mongoConfig, clusterConfig);
		
		this.pool = Executors.newCachedThreadPool(new LumongoThreadFactory("manager"));
		
	}
	
	public void init(HazelcastManager hazelcastManager) throws UnknownHostException, MongoException {
		globalLock.writeLock().lock();
		try {
			this.hazelcastManager = hazelcastManager;
			
			String databaseName = mongoConfig.getDatabaseName();
			String mongoHost = mongoConfig.getMongoHost();
			int mongoPort = mongoConfig.getMongoPort();
			boolean sharded = clusterConfig.isSharded();
			
			MongoOptions options = new MongoOptions();
			options.connectionsPerHost = 15;
			this.mongo = new Mongo(new ServerAddress(mongoHost, mongoPort), options);
			
			if (sharded) {
				DB db = mongo.getDB(MongoConstants.StandardDBs.ADMIN);
				db.command(new BasicDBObject(MongoConstants.Commands.ENABLE_SHARDING, databaseName));
			}
			
			this.documentStorage = new MongoDocumentStorage(mongoHost, mongoPort, databaseName + MongoDocumentStorage.STORAGE_DB_SUFFIX, sharded);
		}
		finally {
			globalLock.writeLock().unlock();
		}
		
	}
	
	public void handleServerRemoved(Set<Member> currentMembers, Member memberRemoved, boolean master) {
		globalLock.writeLock().lock();
		try {
			if (master) {
				handleServerRemoved(currentMembers, memberRemoved);
			}
			
			internalClient.removeMember(memberRemoved);
		}
		finally {
			globalLock.writeLock().unlock();
		}
		
	}
	
	public void handleServerAdded(Set<Member> currentMembers, Member memberAdded, boolean master) throws Exception {
		globalLock.writeLock().lock();
		try {
			if (master) {
				// make sure we can resolve it before transfering segments
				Nodes nodes = ClusterHelper.getNodes(mongoConfig);
				@SuppressWarnings("unused")
				LocalNodeConfig localNodeConfig = nodes.find(memberAdded);
				
				handleServerAdded(currentMembers, memberAdded);
			}
			
			internalClient.addMember(memberAdded);
		}
		finally {
			globalLock.writeLock().unlock();
		}
		
	}
	
	public List<String> getIndexNames() {
		globalLock.writeLock().lock();
		
		try {
			ArrayList<String> indexNames = new ArrayList<String>();
			DB db = mongo.getDB(mongoConfig.getDatabaseName());
			Set<String> allCollections = db.getCollectionNames();
			
			for (String collection : allCollections) {
				if (collection.endsWith(Index.CONFIG_SUFFIX)) {
					String indexName = collection.substring(0, collection.length() - Index.CONFIG_SUFFIX.length());
					indexNames.add(indexName);
				}
			}
			
			return indexNames;
		}
		finally {
			globalLock.writeLock().unlock();
		}
	}
	
	public void loadIndexes() {
		globalLock.writeLock().lock();
		
		try {
			log.info("Loading existing indexes");
			List<String> indexNames = getIndexNames();
			for (String indexName : indexNames) {
				try {
					loadIndex(indexName, true);
				}
				catch (Exception e) {
					log.error("Failed to load index <" + indexName + ">: " + e.getClass().getSimpleName() + ": ", e);
				}
			}
			log.info("Finished loading existing indexes");
		}
		finally {
			globalLock.writeLock().unlock();
		}
	}
	
	public IndexCreateResponse createIndex(IndexCreateRequest request) throws Exception {
		globalLock.writeLock().lock();
		try {
			log.info("Creating index: <" + request.getIndexName() + ">: " + request);
			
			IndexConfig indexConfig = new IndexConfig(request);
			
			String indexName = indexConfig.getIndexName();
			if (indexMap.containsKey(indexName)) {
				throw new Exception("Index <" + indexName + "> already exist");
			}
			Index i = Index.createIndex(hazelcastManager, mongoConfig, clusterConfig, mongo, indexConfig);
			indexMap.put(indexConfig.getIndexName(), i);
			i.loadAllSegments();
			i.forceBalance(hazelcastManager.getMembers());
			
			log.info("Created index: <" + request.getIndexName() + ">");
			
			return IndexCreateResponse.newBuilder().build();
		}
		finally {
			globalLock.writeLock().unlock();
		}
	}
	
	public void loadIndex(String indexName, boolean loadAllSegments) throws Exception {
		globalLock.writeLock().lock();
		try {
			Index i = Index.loadIndex(hazelcastManager, mongoConfig, clusterConfig, mongo, indexName);
			if (loadAllSegments) {
				i.loadAllSegments();
			}
			indexMap.put(indexName, i);
		}
		finally {
			globalLock.writeLock().unlock();
		}
	}
	
	private void handleServerAdded(Set<Member> currentMembers, Member memberAdded) {
		globalLock.writeLock().lock();
		try {
			for (String key : indexMap.keySet()) {
				Index i = indexMap.get(key);
				i.handleServerAdded(currentMembers, memberAdded);
			}
		}
		finally {
			globalLock.writeLock().unlock();
		}
	}
	
	private void handleServerRemoved(Set<Member> currentMembers, Member memberRemoved) {
		globalLock.writeLock().lock();
		try {
			for (String key : indexMap.keySet()) {
				Index i = indexMap.get(key);
				i.handleServerRemoved(currentMembers, memberRemoved);
			}
		}
		finally {
			globalLock.writeLock().unlock();
		}
	}
	
	public void updateSegmentMap(String indexName, Map<Member, Set<Integer>> newMemberToSegmentMap) throws Exception {
		globalLock.writeLock().lock();
		try {
			if (!indexMap.containsKey(indexName)) {
				loadIndex(indexName, false);
			}
			
			Index i = indexMap.get(indexName);
			if (i == null) {
				throw new IndexDoesNotExist(indexName);
			}
			i.updateSegmentMap(newMemberToSegmentMap);
		}
		finally {
			globalLock.writeLock().unlock();
		}
	}
	
	public IndexDeleteResponse deleteIndex(IndexDeleteRequest request) throws Exception {
		globalLock.writeLock().lock();
		try {
			String indexName = request.getIndexName();
			
			Index i = indexMap.get(indexName);
			if (i == null) {
				throw new IndexDoesNotExist(indexName);
			}
			
			Set<Member> currentMembers = hazelcastManager.getMembers();
			ExecutorService executorService = hazelcastManager.getExecutorService();
			
			Member self = hazelcastManager.getSelf();
			
			log.info("Unload index <" + indexName + "> for delete");
			for (Member m : currentMembers) {
				try {
					UnloadIndexTask uit = new UnloadIndexTask(m.getInetSocketAddress().getPort(), indexName);
					if (!self.equals(m)) {
						DistributedTask<Void> dt = new DistributedTask<Void>(uit, m);
						executorService.execute(dt);
						dt.get();
					}
					else {
						uit.call();
					}
				}
				catch (Exception e) {
					log.error(e.getClass().getSimpleName() + ": ", e);
				}
				
			}
			
			log.info("Deleting index <" + indexName + ">");
			i.deleteIndex(mongo);
			indexMap.remove(indexName);
			
			return IndexDeleteResponse.newBuilder().build();
		}
		finally {
			globalLock.writeLock().unlock();
		}
	}
	
	public void unloadIndex(String indexName) throws IndexDoesNotExist, CorruptIndexException, IOException {
		globalLock.writeLock().lock();
		try {
			Index i = indexMap.get(indexName);
			if (i == null) {
				throw new IndexDoesNotExist(indexName);
			}
			
			i.unload();
			indexMap.remove(indexName);
		}
		finally {
			globalLock.writeLock().unlock();
		}
	}
	
	public void shutdown() {
		
		//TODO configure or force a lock acquire
		int waitSeconds = 10;
		
		log.info("Waiting for lock");
		boolean locked = false;
		try {
			locked = globalLock.writeLock().tryLock(waitSeconds, TimeUnit.SECONDS);
		}
		catch (InterruptedException e1) {
			
		}
		
		if (!locked) {
			log.info("Failed to get manager lock within <" + waitSeconds + "> seconds");
		}
		
		try {
			log.info("Stopping manager pool");
			pool.shutdownNow();
			
			log.info("Shutting down indexes");
			for (String indexName : indexMap.keySet()) {
				Index i = indexMap.get(indexName);
				try {
					log.info("Unloading <" + indexName + ">");
					i.unload();
				}
				catch (Exception e) {
					log.error(e.getClass().getSimpleName() + ": ", e);
				}
			}
		}
		finally {
			if (locked) {
				globalLock.writeLock().unlock();
			}
		}
	}
	
	public void openConnections(Set<Member> members) throws Exception {
		
		globalLock.writeLock().lock();
		try {
			Member self = hazelcastManager.getSelf();
			for (Member m : members) {
				if (!self.equals(m)) {
					internalClient.addMember(m);
				}
			}
		}
		finally {
			globalLock.writeLock().unlock();
		}
		
	}
	
	public IndexSettingsResponse updateIndex(String indexName, IndexSettings request) throws IndexDoesNotExist, InvalidIndexConfig, MongoException, IOException {
		globalLock.readLock().lock();
		try {
			
			Index i = indexMap.get(indexName);
			if (i == null) {
				throw new IndexDoesNotExist(indexName);
			}
			
			i.updateIndexSettings(request);
			
			Set<Member> currentMembers = hazelcastManager.getMembers();
			ExecutorService executorService = hazelcastManager.getExecutorService();
			
			Member self = hazelcastManager.getSelf();
			
			for (Member m : currentMembers) {
				try {
					ReloadIndexSettingsTask rist = new ReloadIndexSettingsTask(m.getInetSocketAddress().getPort(), indexName);
					if (!self.equals(m)) {
						DistributedTask<Void> dt = new DistributedTask<Void>(rist, m);
						executorService.execute(dt);
						dt.get();
					}
					else {
						rist.call();
					}
				}
				catch (Exception e) {
					log.error(e.getClass().getSimpleName() + ": ", e);
				}
				
			}
			
			return IndexSettingsResponse.newBuilder().build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public void reloadIndexSettings(String indexName) throws Exception {
		globalLock.readLock().lock();
		try {
			
			Index i = indexMap.get(indexName);
			if (i == null) {
				throw new IndexDoesNotExist(indexName);
			}
			
			i.reloadIndexSettings();
			
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public InternalDeleteResponse internalDeleteFromIndex(String uniqueId, Collection<String> indexNames) throws IndexDoesNotExist, CorruptIndexException,
			SegmentDoesNotExist, IOException {
		globalLock.readLock().lock();
		try {
			for (String indexName : indexNames) {
				Index i = indexMap.get(indexName);
				if (i == null) {
					throw new IndexDoesNotExist(indexName);
				}
				i.deleteFromIndex(uniqueId);
			}
			return InternalDeleteResponse.newBuilder().build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	private void deleteFromIndex(String uniqueId, Collection<String> indexes) throws IndexDoesNotExist, CorruptIndexException, SegmentDoesNotExist,
			IOException, Exception {
		globalLock.readLock().lock();
		try {
			HashMap<Member, Set<String>> deleteForMember = new HashMap<Member, Set<String>>();
			for (String indexName : indexes) {
				Index i = indexMap.get(indexName);
				if (i == null) {
					throw new IndexDoesNotExist(indexName);
				}
				
				Member m = i.findMember(uniqueId);
				if (!deleteForMember.containsKey(m)) {
					deleteForMember.put(m, new HashSet<String>());
				}
				deleteForMember.get(m).add(indexName);
			}
			
			Member self = hazelcastManager.getSelf();
			
			for (Member m : deleteForMember.keySet()) {
				
				if (!self.equals(m)) {
					InternalDeleteRequest.Builder idr = InternalDeleteRequest.newBuilder();
					idr.addAllIndexes(deleteForMember.get(m));
					idr.setUniqueId(uniqueId);
					internalClient.executeDelete(m, idr.build());
				}
				else {
					internalDeleteFromIndex(uniqueId, deleteForMember.get(m));
				}
			}
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public DeleteResponse deleteDocument(DeleteRequest deleteRequest) throws Exception {
		globalLock.readLock().lock();
		try {
			
			String uniqueId = deleteRequest.getUniqueId();
			
			if (deleteRequest.getDeleteDocument()) {
				// TODO: only delete from indexes document is in?
				deleteFromIndex(uniqueId, indexMap.keySet());
				documentStorage.deleteSourceDocument(uniqueId);
			}
			else if (deleteRequest.getIndexesCount() != 0) {
				deleteFromIndex(uniqueId, deleteRequest.getIndexesList());
			}
			
			if (deleteRequest.getDeleteAllAssociated()) {
				documentStorage.deleteAssociatedDocuments(uniqueId);
			}
			else if (deleteRequest.hasFilename()) {
				String fileName = deleteRequest.getFilename();
				documentStorage.deleteAssociatedDocument(uniqueId, fileName);
			}
			
			return DeleteResponse.newBuilder().build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public InternalIndexResponse internalIndex(String uniqueId, List<LMDoc> indexedDocuments) throws Exception {
		globalLock.readLock().lock();
		try {
			for (LMDoc indexedDocument : indexedDocuments) {
				String indexName = indexedDocument.getIndexName();
				Index i = indexMap.get(indexName);
				if (i == null) {
					throw new IndexDoesNotExist(indexName);
				}
				i.storeInternal(uniqueId, indexedDocument);
			}
			
			return InternalIndexResponse.newBuilder().build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public StoreResponse storeDocument(StoreRequest storeRequest) throws Exception {
		globalLock.readLock().lock();
		try {
			
			String uniqueId = storeRequest.getUniqueId();
			if (storeRequest.getIndexedDocumentCount() > 0) {
				Set<String> indexNames = new HashSet<String>();
				HashMap<Member, List<LMDoc>> indexForMember = new HashMap<Member, List<LMDoc>>();
				for (LMDoc lmDoc : storeRequest.getIndexedDocumentList()) {
					String indexName = lmDoc.getIndexName();
					if (indexNames.contains(indexName)) {
						throw new Exception("Can not store a document for twice for index <" + indexName + ">");
					}
					indexNames.add(indexName);
					
					Index i = indexMap.get(indexName);
					if (i == null) {
						throw new IndexDoesNotExist(indexName);
					}
					Member m = i.findMember(uniqueId);
					
					if (!indexForMember.containsKey(m)) {
						indexForMember.put(m, new ArrayList<LMDoc>());
					}
					indexForMember.get(m).add(lmDoc);
				}
				
				// TODO: only delete from indexes document is in? configurable?
				{
					Set<String> deleteIndexNames = new HashSet<String>();
					for (String indexName : indexMap.keySet()) {
						if (!indexNames.contains(indexName)) {
							deleteIndexNames.add(indexName);
						}
					}
					
					if (!deleteIndexNames.isEmpty()) {
						deleteFromIndex(uniqueId, deleteIndexNames);
					}
					
				}
				
				Member self = hazelcastManager.getSelf();
				
				for (Member m : indexForMember.keySet()) {
					
					if (!self.equals(m)) {
						InternalIndexRequest.Builder iir = InternalIndexRequest.newBuilder();
						iir.setUniqueId(uniqueId);
						iir.addAllIndexedDocument(indexForMember.get(m));
						internalClient.executeIndex(m, iir.build());
					}
					else {
						internalIndex(uniqueId, indexForMember.get(m));
					}
				}
			}
			
			if (storeRequest.getClearExistingAssociated()) {
				documentStorage.deleteAssociatedDocuments(uniqueId);
			}
			
			if (storeRequest.hasResultDocument()) {
				documentStorage.storeSourceDocument(uniqueId, storeRequest.getResultDocument());
			}
			
			for (AssociatedDocument ad : storeRequest.getAssociatedDocumentList()) {
				documentStorage.storeAssociatedDocument(ad);
			}
			
			return StoreResponse.newBuilder().build();
		}
		finally {
			globalLock.readLock().unlock();
		}
		
	}
	
	public FetchResponse fetch(FetchRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			
			FetchResponse.Builder frBuilder = FetchResponse.newBuilder();
			
			if (!FetchType.NONE.equals(request.getResultFetchType())) {
				ResultDocument resultDoc = documentStorage.getSourceDocument(request.getUniqueId(), request.getResultFetchType());
				if (null != resultDoc) {
					frBuilder.setResultDocument(resultDoc);
				}
			}
			if (!FetchType.NONE.equals(request.getAssociatedFetchType())) {
				if (request.hasFileName()) {
					AssociatedDocument ad = documentStorage.getAssociatedDocument(request.getUniqueId(), request.getFileName(),
							request.getAssociatedFetchType());
					if (ad != null) {
						frBuilder.addAssociatedDocument(ad);
					}
				}
				else {
					for (AssociatedDocument ad : documentStorage.getAssociatedDocuments(request.getUniqueId(), request.getAssociatedFetchType())) {
						frBuilder.addAssociatedDocument(ad);
					}
				}
			}
			return frBuilder.build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	private Map<String, Query> getQueryMap(Collection<String> indexNames, String queryString) throws Exception {
		globalLock.readLock().lock();
		try {
			HashMap<String, Query> queryMap = new HashMap<String, Query>();
			for (String indexName : indexNames) {
				Index i = indexMap.get(indexName);
				if (i == null) {
					throw new IndexDoesNotExist(indexName);
				}
				Query query = i.getQuery(queryString);
				queryMap.put(indexName, query);
			}
			
			return queryMap;
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public QueryResponse query(final QueryRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			
			final Map<String, Query> queryMap = getQueryMap(request.getIndexList(), request.getQuery());
			
			final Map<String, Index> indexSegmentMap = new HashMap<String, Index>();
			for (String indexName : request.getIndexList()) {
				Index i = indexMap.get(indexName);
				if (i == null) {
					throw new IndexDoesNotExist(indexName);
				}
				indexSegmentMap.put(indexName, i);
			}
			
			SocketRequestFederator<QueryRequest, InternalQueryResponse> queryFederator = new SocketRequestFederator<QueryRequest, InternalQueryResponse>(
					hazelcastManager, pool) {
				
				@Override
				public InternalQueryResponse processExternal(Member m, QueryRequest request) throws Exception {
					return internalClient.executeQuery(m, request);
				}
				
				@Override
				public InternalQueryResponse processInternal(QueryRequest request) throws Exception {
					return internalQuery(queryMap, request);
				}
			};
			
			List<InternalQueryResponse> results = queryFederator.send(request);
			
			QueryCombiner queryCombiner = new QueryCombiner(indexSegmentMap, request.getAmount(), results, request.getLastResult());
			
			queryCombiner.validate();
			
			QueryResponse qr = queryCombiner.getQueryResponse();
			
			if (!queryCombiner.isShort()) {
				return qr;
			}
			else {
				queryCombiner.logShort();
				
				if (!request.getFetchFull()) {
					return query(request.toBuilder().setFetchFull(true).build());
				}
				
				throw new Exception("Full fetch request is short");
			}
			
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public InternalQueryResponse internalQuery(QueryRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			Map<String, Query> queryMap = getQueryMap(request.getIndexList(), request.getQuery());
			return internalQuery(queryMap, request);
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	private InternalQueryResponse internalQuery(Map<String, Query> queryMap, QueryRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			
			InternalQueryResponse.Builder internalQueryResponseBuilder = InternalQueryResponse.newBuilder();
			for (String indexName : queryMap.keySet()) {
				
				Index i = indexMap.get(indexName);
				if (i == null) {
					throw new IndexDoesNotExist(indexName);
				}
				Query query = queryMap.get(indexName);
				IndexSegmentResponse isr = i.queryInternal(query, request);
				internalQueryResponseBuilder.addIndexSegmentResponse(isr);
			}
			
			return internalQueryResponseBuilder.build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public GetIndexesResponse getIndexes(GetIndexesRequest request) {
		globalLock.readLock().lock();
		try {
			GetIndexesResponse.Builder girB = GetIndexesResponse.newBuilder();
			girB.addAllIndexName(indexMap.keySet());
			return girB.build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public GetNumberOfDocsResponse getNumberOfDocsInternal(GetNumberOfDocsRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			String indexName = request.getIndexName();
			Index i = indexMap.get(indexName);
			if (i == null) {
				throw new IndexDoesNotExist(indexName);
			}
			return i.getNumberOfDocs();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public GetNumberOfDocsResponse getNumberOfDocs(GetNumberOfDocsRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			String indexName = request.getIndexName();
			Index i = indexMap.get(indexName);
			if (i == null) {
				throw new IndexDoesNotExist(indexName);
			}
			int numberOfSegments = i.getNumberOfSegments();
			
			SocketRequestFederator<GetNumberOfDocsRequest, GetNumberOfDocsResponse> federator = new SocketRequestFederator<GetNumberOfDocsRequest, GetNumberOfDocsResponse>(
					hazelcastManager, pool) {
				
				@Override
				public GetNumberOfDocsResponse processExternal(Member m, GetNumberOfDocsRequest request) throws Exception {
					return internalClient.getNumberOfDocs(m, request);
				}
				
				@Override
				public GetNumberOfDocsResponse processInternal(GetNumberOfDocsRequest request) throws Exception {
					return getNumberOfDocsInternal(request);
				}
				
			};
			
			GetNumberOfDocsResponse.Builder responseBuilder = GetNumberOfDocsResponse.newBuilder();
			responseBuilder.setNumberOfDocs(0);
			List<GetNumberOfDocsResponse> responses = federator.send(request);
			for (GetNumberOfDocsResponse r : responses) {
				responseBuilder.setNumberOfDocs(responseBuilder.getNumberOfDocs() + r.getNumberOfDocs());
				responseBuilder.addAllSegmentCountResponse(r.getSegmentCountResponseList());
			}
			
			GetNumberOfDocsResponse response = responseBuilder.build();
			
			HashSet<Integer> segments = new HashSet<Integer>();
			
			for (SegmentCountResponse r : response.getSegmentCountResponseList()) {
				segments.add(r.getSegmentNumber());
			}
			
			if (segments.size() != numberOfSegments) {
				throw new Exception("Expected <" + numberOfSegments + "> segments, found <" + segments.size() + "> segments");
			}
			
			for (int segmentNumber = 0; segmentNumber < numberOfSegments; segmentNumber++) {
				if (!segments.contains(segmentNumber)) {
					throw new Exception("Missing results for segment <" + segmentNumber + ">");
				}
			}
			
			return response;
			
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public ClearResponse clearIndex(ClearRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			SocketRequestFederator<ClearRequest, ClearResponse> federator = new SocketRequestFederator<ClearRequest, ClearResponse>(hazelcastManager, pool) {
				
				@Override
				public ClearResponse processExternal(Member m, ClearRequest request) throws Exception {
					return internalClient.clear(m, request);
				}
				
				@Override
				public ClearResponse processInternal(ClearRequest request) throws Exception {
					return clearInternal(request);
				}
				
			};
			
			// nothing in responses currently
			@SuppressWarnings("unused")
			List<ClearResponse> responses = federator.send(request);
			
			return ClearResponse.newBuilder().build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public ClearResponse clearInternal(ClearRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			String indexName = request.getIndexName();
			Index i = indexMap.get(indexName);
			if (i == null) {
				throw new IndexDoesNotExist(indexName);
			}
			i.clear();
			return ClearResponse.newBuilder().build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public OptimizeResponse optimize(OptimizeRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			SocketRequestFederator<OptimizeRequest, OptimizeResponse> federator = new SocketRequestFederator<OptimizeRequest, OptimizeResponse>(
					hazelcastManager, pool) {
				
				@Override
				public OptimizeResponse processExternal(Member m, OptimizeRequest request) throws Exception {
					return internalClient.optimize(m, request);
				}
				
				@Override
				public OptimizeResponse processInternal(OptimizeRequest request) throws Exception {
					return optimizeInternal(request);
				}
				
			};
			
			// nothing in responses currently
			@SuppressWarnings("unused")
			List<OptimizeResponse> responses = federator.send(request);
			
			return OptimizeResponse.newBuilder().build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public OptimizeResponse optimizeInternal(OptimizeRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			String indexName = request.getIndexName();
			Index i = indexMap.get(indexName);
			if (i == null) {
				throw new IndexDoesNotExist(indexName);
			}
			i.optimize();
			return OptimizeResponse.newBuilder().build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public GetFieldNamesResponse getFieldNames(GetFieldNamesRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			SocketRequestFederator<GetFieldNamesRequest, GetFieldNamesResponse> federator = new SocketRequestFederator<GetFieldNamesRequest, GetFieldNamesResponse>(
					hazelcastManager, pool) {
				
				@Override
				public GetFieldNamesResponse processExternal(Member m, GetFieldNamesRequest request) throws Exception {
					return internalClient.getFieldNames(m, request);
				}
				
				@Override
				public GetFieldNamesResponse processInternal(GetFieldNamesRequest request) throws Exception {
					return getFieldNamesInternal(request);
				}
				
			};
			
			Set<String> fieldNames = new HashSet<String>();
			List<GetFieldNamesResponse> responses = federator.send(request);
			for (GetFieldNamesResponse response : responses) {
				fieldNames.addAll(response.getFieldNameList());
			}
			
			GetFieldNamesResponse.Builder responseBuilder = GetFieldNamesResponse.newBuilder();
			responseBuilder.addAllFieldName(fieldNames);
			return responseBuilder.build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public GetFieldNamesResponse getFieldNamesInternal(GetFieldNamesRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			String indexName = request.getIndexName();
			Index i = indexMap.get(indexName);
			if (i == null) {
				throw new IndexDoesNotExist(indexName);
			}
			return i.getFieldNames();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public GetTermsResponse getTerms(GetTermsRequest request) throws Exception {
		
		globalLock.readLock().lock();
		try {
			
			SocketRequestFederator<GetTermsRequest, GetTermsResponse> federator = new SocketRequestFederator<GetTermsRequest, GetTermsResponse>(
					hazelcastManager, pool) {
				
				@Override
				public GetTermsResponse processExternal(Member m, GetTermsRequest request) throws Exception {
					return internalClient.getTerms(m, request);
				}
				
				@Override
				public GetTermsResponse processInternal(GetTermsRequest request) throws Exception {
					return getTermsInternal(request);
				}
				
			};
			
			List<GetTermsResponse> responses = federator.send(request);
			
			//not threaded but atomic long is convenient
			//TODO: maybe change to something else for speed
			TreeMap<String, AtomicLong> terms = new TreeMap<String, AtomicLong>();
			for (GetTermsResponse gtr : responses) {
				for (Term term : gtr.getTermList()) {
					if (!terms.containsKey(term.getValue())) {
						terms.put(term.getValue(), new AtomicLong());
					}
					terms.get(term.getValue()).addAndGet(term.getDocFreq());
				}
			}
			
			GetTermsResponse.Builder responseBuilder = GetTermsResponse.newBuilder();
			
			int amountToReturn = Math.min(request.getAmount(), terms.size());
			
			String value = null;
			Long frequency = null;
			
			for (int i = 0; i < amountToReturn && !terms.isEmpty();) {
				value = terms.firstKey();
				AtomicLong docFreq = terms.remove(value);
				frequency = docFreq.get();
				if (frequency >= request.getMinDocFreq()) {
					i++;
					responseBuilder.addTerm(Lumongo.Term.newBuilder().setValue(value).setDocFreq(frequency));
				}
			}
			if (value != null && frequency != null) {
				responseBuilder.setLastTerm(Lumongo.Term.newBuilder().setValue(value).setDocFreq(frequency).build());
			}
			
			return responseBuilder.build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public GetTermsResponse getTermsInternal(GetTermsRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			String indexName = request.getIndexName();
			Index i = indexMap.get(indexName);
			if (i == null) {
				throw new IndexDoesNotExist(indexName);
			}
			return i.getTerms(request);
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, boolean compress, HashMap<String, String> metadataMap)
			throws Exception {
		globalLock.readLock().lock();
		try {
			documentStorage.storeAssociatedDocument(uniqueId, fileName, is, compress, metadataMap);
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public InputStream getAssociatedDocumentStream(String uniqueId, String fileName) {
		globalLock.readLock().lock();
		try {
			return documentStorage.getAssociatedDocumentStream(uniqueId, fileName);
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
	public GetMembersResponse getMembers(GetMembersRequest request) throws Exception {
		globalLock.readLock().lock();
		try {
			Set<Member> members = hazelcastManager.getMembers();
			GetMembersResponse.Builder responseBuilder = GetMembersResponse.newBuilder();
			
			Nodes nodes = ClusterHelper.getNodes(mongoConfig);
			
			for (Member m : members) {
				LocalNodeConfig localNodeConfig = nodes.find(m);
				
				InetAddress inetAddress = m.getInetSocketAddress().getAddress();
				
				String fullHostName = inetAddress.getCanonicalHostName();
				
				LMMember.Builder lmMemberBuilder = LMMember.newBuilder();
				lmMemberBuilder.setServerAddress(fullHostName);
				lmMemberBuilder.setExternalPort(localNodeConfig.getExternalServicePort());
				lmMemberBuilder.setInternalPort(localNodeConfig.getInternalServicePort());
				lmMemberBuilder.setHazelcastPort(localNodeConfig.getHazelcastPort());
				lmMemberBuilder.setRestPort(localNodeConfig.getRestPort());
				responseBuilder.addMember(lmMemberBuilder.build());
				
			}
			
			return responseBuilder.build();
		}
		finally {
			globalLock.readLock().unlock();
		}
	}
	
}
