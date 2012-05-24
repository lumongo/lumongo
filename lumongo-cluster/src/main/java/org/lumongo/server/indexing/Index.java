package org.lumongo.server.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.lumongo.LuceneConstants;
import org.lumongo.analyzer.LowercaseKeywordAnalyzer;
import org.lumongo.analyzer.LowercaseWhitespaceAnalyzer;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsResponse;
import org.lumongo.cluster.message.Lumongo.GetTermsRequest;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;
import org.lumongo.cluster.message.Lumongo.IndexSegmentResponse;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LastIndexResult;
import org.lumongo.cluster.message.Lumongo.LastResult;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.cluster.message.Lumongo.SegmentCountResponse;
import org.lumongo.cluster.message.Lumongo.SegmentResponse;
import org.lumongo.server.config.ClusterConfig;
import org.lumongo.server.config.IndexConfig;
import org.lumongo.server.config.MongoConfig;
import org.lumongo.server.exceptions.InvalidIndexConfig;
import org.lumongo.server.exceptions.SegmentDoesNotExist;
import org.lumongo.server.hazelcast.HazelcastManager;
import org.lumongo.server.hazelcast.UpdateSegmentsTask;
import org.lumongo.storage.constants.MongoConstants;
import org.lumongo.storage.lucene.DistributedDirectory;
import org.lumongo.storage.lucene.MongoDirectory;
import org.lumongo.util.LumongoThreadFactory;

import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.ILock;
import com.hazelcast.core.Member;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class Index {
	private final static Logger log = Logger.getLogger(Index.class);
	private static final String SETTINGS_ID = "settings";
	public static final String CONFIG_SUFFIX = "_config";
	
	private IndexConfig indexConfig;
	private final Mongo mongo;
	private final MongoConfig mongoConfig;
	private final ClusterConfig clusterConfig;
	
	private final GenericObjectPool<QueryParser> parsers;
	
	private Map<Member, Set<Integer>> memberToSegmentMap;
	private Map<Integer, Member> segmentToMemberMap;
	
	private final ConcurrentHashMap<Integer, Segment> segmentMap;
	private final ConcurrentHashMap<Integer, ILock> hazelLockMap;
	
	private final ReadWriteLock indexLock;
	
	private Timer commitTimer;
	
	private final ExecutorService segmentPool;
	
	private final int numberOfSegments;
	private final String indexName;
	
	private TimerTask commitTask;
	
	private final HazelcastManager hazelcastManager;
	
	private Index(HazelcastManager hazelcastManger, MongoConfig mongoConfig, ClusterConfig clusterConfig, Mongo mongo, IndexConfig indexConfig) {
		this.hazelcastManager = hazelcastManger;
		
		this.mongoConfig = mongoConfig;
		this.clusterConfig = clusterConfig;
		
		this.mongo = mongo;
		this.indexConfig = indexConfig;
		this.indexName = indexConfig.getIndexName();
		this.numberOfSegments = indexConfig.getNumberOfSegments();
		
		this.segmentPool = Executors.newCachedThreadPool(new LumongoThreadFactory(indexName + "-segments"));
		
		this.parsers = new GenericObjectPool<QueryParser>(new BasePoolableObjectFactory<QueryParser>() {
			
			@Override
			public QueryParser makeObject() throws Exception {
				return createQueryParser();
			}
			
		});
		
		this.indexLock = new ReentrantReadWriteLock(true);
		this.segmentMap = new ConcurrentHashMap<Integer, Segment>();
		this.hazelLockMap = new ConcurrentHashMap<Integer, ILock>();
		
		commitTimer = new Timer(indexName + "-CommitTimer", true);
		
		commitTask = new TimerTask() {
			
			@Override
			public void run() {
				if (Index.this.indexConfig.getIdleTimeWithoutCommit() != 0) {
					doCommit(false);
				}
				
			}
			
		};
		
		commitTimer.scheduleAtFixedRate(commitTask, 1000, 1000);
		
	}
	
	public void updateIndexSettings(IndexSettings request) {
		indexLock.writeLock().lock();
		try {
			log.info("Updating index settings for <" + indexName + ">: " + request);
			indexConfig.configure(request);
			storeIndexSettings();
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}
	
	private QueryParser createQueryParser() throws Exception {
		return new QueryParser(LuceneConstants.VERSION, indexConfig.getDefaultSearchField(), getAnalyzer());
	}
	
	public Analyzer getAnalyzer() throws Exception {
		HashMap<String, Analyzer> customAnalyzerMap = new HashMap<String, Analyzer>();
		for (FieldConfig fieldConfig : indexConfig.getFieldConfigList()) {
			customAnalyzerMap.put(fieldConfig.getFieldName(), getAnalyzer(fieldConfig.getAnalyzer()));
		}
		
		Analyzer defaultAnalyzer = getAnalyzer(indexConfig.getDefaultAnalyzer());
		
		PerFieldAnalyzerWrapper aWrapper = new PerFieldAnalyzerWrapper(defaultAnalyzer, customAnalyzerMap);
		return aWrapper;
	}
	
	protected Analyzer getAnalyzer(LMAnalyzer lmAnalyzer) throws Exception {
		if (LMAnalyzer.KEYWORD.equals(lmAnalyzer)) {
			return new KeywordAnalyzer();
		}
		else if (LMAnalyzer.LC_KEYWORD.equals(lmAnalyzer)) {
			return new LowercaseKeywordAnalyzer();
		}
		else if (LMAnalyzer.WHITESPACE.equals(lmAnalyzer)) {
			return new WhitespaceAnalyzer(LuceneConstants.VERSION);
		}
		else if (LMAnalyzer.LC_WHITESPACE.equals(lmAnalyzer)) {
			return new LowercaseWhitespaceAnalyzer();
		}
		else if (LMAnalyzer.STANDARD.equals(lmAnalyzer)) {
			return new StandardAnalyzer(LuceneConstants.VERSION);
		}
		
		throw new Exception("Unsupport analyzer <" + lmAnalyzer + ">");
		
	}
	
	private void doCommit(boolean force) {
		indexLock.readLock().lock();
		try {
			Collection<Segment> segments = segmentMap.values();
			for (Segment segment : segments) {
				try {
					if (force) {
						segment.forceCommit();
					}
					else {
						segment.doCommit();
					}
				}
				catch (Exception e) {
					log.error("Failed to flushing segment <" + segment.getSegmentNumber() + "> for index <" + indexName + ">: " + e.getClass().getSimpleName()
							+ ": ", e);
				}
			}
		}
		finally {
			indexLock.readLock().unlock();
		}
		
	}
	
	public void updateSegmentMap(Map<Member, Set<Integer>> newMemberToSegmentMap) {
		indexLock.writeLock().lock();
		try {
			log.info("Updating segments map");
			
			this.memberToSegmentMap = newMemberToSegmentMap;
			this.segmentToMemberMap = new HashMap<Integer, Member>();
			
			for (Member m : memberToSegmentMap.keySet()) {
				for (int i : memberToSegmentMap.get(m)) {
					segmentToMemberMap.put(i, m);
				}
			}
			
			Member self = hazelcastManager.getSelf();
			
			Set<Integer> newSegments = memberToSegmentMap.get(self);
			
			log.info("Settings segments for this node <" + self + "> to <" + newSegments + ">");
			
			for (Integer segmentNumber : segmentMap.keySet()) {
				if (!newSegments.contains(segmentNumber)) {
					try {
						unloadSegment(segmentNumber);
					}
					catch (Exception e) {
						log.error("Error unloading segment <" + segmentNumber + "> for index <" + indexName + ">");
						log.error(e.getClass().getSimpleName() + ": ", e);
					}
				}
			}
			
			for (Integer segmentNumber : newSegments) {
				if (!segmentMap.containsKey(segmentNumber)) {
					try {
						loadSegment(segmentNumber);
					}
					catch (Exception e) {
						log.error("Error loading segment <" + segmentNumber + "> for index <" + indexName + ">");
						log.error(e.getClass().getSimpleName() + ": ", e);
					}
				}
			}
			
		}
		finally {
			indexLock.writeLock().unlock();
		}
		
	}
	
	public void loadAllSegments() throws Exception {
		indexLock.writeLock().lock();
		try {
			Member self = hazelcastManager.getSelf();
			this.memberToSegmentMap = new HashMap<Member, Set<Integer>>();
			this.memberToSegmentMap.put(self, new HashSet<Integer>());
			for (int segmentNumber = 0; segmentNumber < numberOfSegments; segmentNumber++) {
				loadSegment(segmentNumber);
				this.memberToSegmentMap.get(self).add(segmentNumber);
			}
			
			this.segmentToMemberMap = new HashMap<Integer, Member>();
			
			for (Member m : memberToSegmentMap.keySet()) {
				for (int i : memberToSegmentMap.get(m)) {
					segmentToMemberMap.put(i, m);
				}
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}
	
	public void unload() throws CorruptIndexException, IOException {
		indexLock.writeLock().lock();
		try {
			log.info("Canceling timers for <" + indexName + ">");
			commitTask.cancel();
			commitTimer.cancel();
			log.info("Commiting <" + indexName + ">");
			doCommit(true);
			
			log.info("Shutting segment pool for <" + indexName + ">");
			segmentPool.shutdownNow();
			
			for (Integer segmentNumber : segmentMap.keySet()) {
				unloadSegment(segmentNumber);
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}
	
	private void loadSegment(int segmentNumber) throws Exception {
		indexLock.writeLock().lock();
		try {
			if (!segmentMap.containsKey(segmentNumber)) {
				String lockName = indexName + "-" + segmentNumber;
				ILock hzLock = hazelcastManager.getLock(lockName);
				hazelLockMap.put(segmentNumber, hzLock);
				log.info("Waiting for lock for index <" + indexName + "> segment <" + segmentNumber + ">");
				hzLock.lock();
				log.info("Obtained lock for index <" + indexName + "> segment <" + segmentNumber + ">");
				
				MongoDirectory mongoDirectory = new MongoDirectory(mongo, mongoConfig.getDatabaseName(), indexName + "_" + segmentNumber,
						clusterConfig.isSharded(), indexConfig.isBlockCompression(), clusterConfig.getIndexBlockSize());
				DistributedDirectory dd = new DistributedDirectory(mongoDirectory);
				
				IndexWriterConfig config = new IndexWriterConfig(LuceneConstants.VERSION, getAnalyzer());
				//TODO configurable, or use flush interval				
				//config.setRAMBufferSizeMB(32);
				
				IndexWriter indexWriter = new IndexWriter(dd, config);
				
				Segment s = new Segment(segmentNumber, indexWriter, indexConfig);
				segmentMap.put(segmentNumber, s);
				
				log.info("Loaded segment <" + segmentNumber + "> for index <" + indexName + ">");
				log.info("Current segments <" + (new TreeSet<Integer>(segmentMap.keySet())) + "> for index <" + indexName + ">");
				
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}
	
	public void unloadSegment(int segmentNumber) throws CorruptIndexException, IOException {
		indexLock.writeLock().lock();
		try {
			ILock hzLock = hazelLockMap.get(segmentNumber);
			try {
				if (segmentMap.containsKey(segmentNumber)) {
					Segment s = segmentMap.remove(segmentNumber);
					if (s != null) {
						log.info("Commiting and closing segment <" + segmentNumber + "> for index <" + indexName + ">");
						s.close();
						log.info("Removed segment <" + segmentNumber + "> for index <" + indexName + ">");
						log.info("Current segments <" + (new TreeSet<Integer>(segmentMap.keySet())) + "> for index <" + indexName + ">");
					}
				}
				
			}
			finally {
				try {
					hzLock.unlock();
					hzLock.forceUnlock();
					log.info("Unlocked lock for index <" + indexName + "> segment <" + segmentNumber + ">");
				}
				catch (Exception e) {
					log.error("Failed to unlock <" + segmentNumber + ">");
				}
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}
		
	}
	
	/**
	 * called on older cluster node when a new member is added
	 * 
	 * @param currentMembers
	 *            - current cluster members
	 * @param memberAdded
	 *            - member that is being added
	 */
	public void handleServerAdded(Set<Member> currentMembers, Member memberAdded) {
		indexLock.writeLock().lock();
		try {
			forceBalance(currentMembers);
		}
		finally {
			indexLock.writeLock().unlock();
		}
		
	}
	
	public void forceBalance(Set<Member> currentMembers) {
		indexLock.writeLock().lock();
		try {
			mapSanityCheck(currentMembers);
			balance(currentMembers);
			
			ExecutorService executorService = hazelcastManager.getExecutorService();
			
			List<DistributedTask<Void>> tasks = new ArrayList<DistributedTask<Void>>();
			
			for (Member m : currentMembers) {
				
				try {
					UpdateSegmentsTask ust = new UpdateSegmentsTask(m.getInetSocketAddress().getPort(), indexName, memberToSegmentMap);
					if (!m.localMember()) {
						DistributedTask<Void> dt = new DistributedTask<Void>(ust, m);
						executorService.execute(dt);
						tasks.add(dt);
					}
				}
				catch (Exception e) {
					log.error(e.getClass().getSimpleName() + ": ", e);
				}
				
			}
			
			try {
				UpdateSegmentsTask ust = new UpdateSegmentsTask(hazelcastManager.getHazelcastPort(), indexName, memberToSegmentMap);
				ust.call();
			}
			catch (Exception e) {
				log.error(e.getClass().getSimpleName() + ": ", e);
			}
			for (DistributedTask<Void> task : tasks) {
				try {
					task.get();
				}
				catch (Exception e) {
					log.error(e.getClass().getSimpleName() + ": ", e);
				}
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}
		
	}
	
	/**
	 * Called on older cluster node when member is removed
	 * 
	 * @param currentMembers
	 *            - current cluster members
	 * @param memberRemoved
	 *            - member that is being removed
	 */
	public void handleServerRemoved(Set<Member> currentMembers, Member memberRemoved) {
		indexLock.writeLock().lock();
		try {
			Set<Integer> segmentsToRedist = memberToSegmentMap.remove(memberRemoved);
			if (segmentsToRedist != null) {
				Member first = currentMembers.iterator().next();
				memberToSegmentMap.get(first).addAll(segmentsToRedist);
			}
			
			forceBalance(currentMembers);
		}
		finally {
			indexLock.writeLock().unlock();
		}
		
	}
	
	private void balance(Set<Member> currentMembers) {
		indexLock.writeLock().lock();
		try {
			boolean balanced = false;
			do {
				int minSegmentsForMember = Integer.MAX_VALUE;
				int maxSegmentsForMember = Integer.MIN_VALUE;
				Member minMember = null;
				Member maxMember = null;
				for (Member m : currentMembers) {
					int segmentsForMemberCount = 0;
					Set<Integer> segmentsForMember = memberToSegmentMap.get(m);
					if (segmentsForMember != null) {
						segmentsForMemberCount = segmentsForMember.size();
					}
					if (segmentsForMemberCount < minSegmentsForMember) {
						minSegmentsForMember = segmentsForMemberCount;
						minMember = m;
					}
					if (segmentsForMemberCount > maxSegmentsForMember) {
						maxSegmentsForMember = segmentsForMemberCount;
						maxMember = m;
					}
				}
				
				if (maxSegmentsForMember - minSegmentsForMember > 1) {
					int valueToMove = memberToSegmentMap.get(maxMember).iterator().next();
					
					log.info("Moving segment <" + valueToMove + "> from <" + maxMember + "> to <" + minMember + ">");
					memberToSegmentMap.get(maxMember).remove(valueToMove);
					
					if (!memberToSegmentMap.containsKey(minMember)) {
						memberToSegmentMap.put(minMember, new HashSet<Integer>());
					}
					
					memberToSegmentMap.get(minMember).add(valueToMove);
				}
				else {
					balanced = true;
				}
				
			}
			while (!balanced);
		}
		finally {
			indexLock.writeLock().unlock();
		}
		
	}
	
	private void mapSanityCheck(Set<Member> currentMembers) {
		indexLock.writeLock().lock();
		try {
			// add all segments to a set
			Set<Integer> allSegments = new HashSet<Integer>();
			for (int segment = 0; segment < indexConfig.getNumberOfSegments(); segment++) {
				allSegments.add(segment);
			}
			
			// ensure all members are in the map and contain an empty set
			for (Member m : currentMembers) {
				if (!memberToSegmentMap.containsKey(m)) {
					memberToSegmentMap.put(m, new HashSet<Integer>());
				}
				if (memberToSegmentMap.get(m) == null) {
					memberToSegmentMap.put(m, new HashSet<Integer>());
				}
			}
			
			// get all members of the map
			Set<Member> mapMembers = memberToSegmentMap.keySet();
			for (Member m : mapMembers) {
				
				// get current segments
				Set<Integer> segments = memberToSegmentMap.get(m);
				
				Set<Integer> invalidSegments = new HashSet<Integer>();
				for (int segment : segments) {
					// check if valid segment
					if (!allSegments.contains(segment)) {
						if (segment < 0 || segment >= indexConfig.getNumberOfSegments()) {
							log.error("Segment <" + segment + "> should not exist for cluster");
						}
						else {
							log.error("Segment <" + segment + "> is duplicated in node <" + m + ">");
						}
						invalidSegments.add(segment);
						
					}
				}
				// remove any invalid segments for the cluster
				segments.removeAll(invalidSegments);
				// remove from all segments to keep track of segments already used
				allSegments.removeAll(segments);
			}
			
			// adds any segments that are missing back to the first node
			if (!allSegments.isEmpty()) {
				log.error("Segments <" + allSegments + "> are missing from the cluster. Adding back in.");
				memberToSegmentMap.values().iterator().next().addAll(allSegments);
			}
		}
		finally {
			indexLock.writeLock().unlock();
		}
		
	}
	
	public Segment findSegmentFromUniqueId(String uniqueId) throws SegmentDoesNotExist {
		indexLock.readLock().lock();
		try {
			int segmentNumber = getSegmentNumberForUniqueId(uniqueId);
			Segment s = segmentMap.get(segmentNumber);
			if (s == null) {
				throw new SegmentDoesNotExist(indexName, segmentNumber);
			}
			return s;
		}
		finally {
			indexLock.readLock().unlock();
		}
	}
	
	public Member findMember(String uniqueId) {
		indexLock.readLock().lock();
		try {
			int segmentNumber = getSegmentNumberForUniqueId(uniqueId);
			Member owner = segmentToMemberMap.get(segmentNumber);
			return owner;
		}
		finally {
			indexLock.readLock().unlock();
		}
	}
	
	private int getSegmentNumberForUniqueId(String uniqueId) {
		int segmentNumber = Math.abs(uniqueId.hashCode()) % indexConfig.getNumberOfSegments();
		return segmentNumber;
	}
	
	public void deleteIndex(Mongo mongo) throws Exception {
		
		DB db = mongo.getDB(mongoConfig.getDatabaseName());
		
		DBCollection dbCollection = db.getCollection(indexName + CONFIG_SUFFIX);
		dbCollection.drop();
		
		for (int i = 0; i < numberOfSegments; i++) {
			String segmentCollection = indexName + "_" + i;
			dbCollection = db.getCollection(segmentCollection);
			dbCollection.drop();
		}
		
	}
	
	public void storeInternal(String uniqueId, LMDoc lmDoc) throws Exception {
		indexLock.readLock().lock();
		
		try {
			Segment s = findSegmentFromUniqueId(uniqueId);
			s.index(uniqueId, lmDoc);
		}
		finally {
			indexLock.readLock().unlock();
		}
	}
	
	public void deleteFromIndex(String uniqueId) throws SegmentDoesNotExist, CorruptIndexException, IOException {
		
		indexLock.readLock().lock();
		
		try {
			
			Segment s = findSegmentFromUniqueId(uniqueId);
			s.delete(uniqueId);
		}
		finally {
			indexLock.readLock().unlock();
		}
	}
	
	public Query getQuery(String query) throws Exception {
		indexLock.readLock().lock();
		
		QueryParser qp = null;
		
		try {
			qp = parsers.borrowObject();
			return qp.parse(query);
		}
		finally {
			try {
				parsers.returnObject(qp);
			}
			finally {
				indexLock.readLock().unlock();
			}
		}
	}
	
	public IndexSegmentResponse queryInternal(final Query query, QueryRequest queryRequest) throws Exception {
		indexLock.readLock().lock();
		try {
			int amount = queryRequest.getAmount();
			if (!queryRequest.getFetchFull()) {
				amount = (int) (((queryRequest.getAmount() / numberOfSegments) + indexConfig.getMinSegmentRequest()) * indexConfig.getRequestFactor());
			}
			
			final int requestedAmount = amount;
			
			final HashMap<Integer, ScoreDoc> lastScoreDocMap = new HashMap<Integer, ScoreDoc>();
			ScoreDoc after = null;
			String indexName = indexConfig.getIndexName();
			LastResult lr = queryRequest.getLastResult();
			if (lr != null) {
				for (LastIndexResult lir : lr.getLastIndexResultList()) {
					if (indexName.equals(lir.getIndexName())) {
						for (ScoredResult sr : lir.getLastForSegmentList()) {
							int docId = sr.getDocId();
							float score = sr.getScore();
							after = new ScoreDoc(docId, score, sr.getSegment());
							lastScoreDocMap.put(sr.getSegment(), after);
						}
					}
				}
			}
			
			IndexSegmentResponse.Builder builder = IndexSegmentResponse.newBuilder();
			
			List<Future<SegmentResponse>> responses = new ArrayList<Future<SegmentResponse>>();
			
			for (final Segment segment : segmentMap.values()) {
				
				Future<SegmentResponse> response = segmentPool.submit(new Callable<SegmentResponse>() {
					
					@Override
					public SegmentResponse call() throws Exception {
						return segment.querySegment(query, requestedAmount, lastScoreDocMap.get(segment.getSegmentNumber()));
					}
					
				});
				
				responses.add(response);
				
			}
			
			for (Future<SegmentResponse> response : responses) {
				try {
					SegmentResponse rs = response.get();
					builder.addSegmentReponse(rs);
				}
				catch (ExecutionException e) {
					throw ((Exception) e.getCause());
				}
			}
			
			builder.setIndexName(indexName);
			return builder.build();
		}
		finally {
			indexLock.readLock().unlock();
		}
		
	}
	
	public Integer getNumberOfSegments() {
		return numberOfSegments;
	}
	
	public double getSegmentTolerance() {
		return indexConfig.getSegmentTolerance();
	}
	
	private void storeIndexSettings() {
		indexLock.writeLock().lock();
		try {
			DB db = mongo.getDB(mongoConfig.getDatabaseName());
			DBCollection dbCollection = db.getCollection(indexConfig.getIndexName() + CONFIG_SUFFIX);
			DBObject settings = indexConfig.toDBObject();
			settings.put(MongoConstants.StandardFields._ID, SETTINGS_ID);
			dbCollection.save(settings);
		}
		finally {
			indexLock.writeLock().unlock();
		}
		
	}
	
	public void reloadIndexSettings() throws InvalidIndexConfig {
		indexLock.writeLock().lock();
		try {
			indexConfig = loadIndexSettings(mongo, mongoConfig.getDatabaseName(), indexName);
			parsers.clear();
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}
	
	public void optimize() throws Exception {
		indexLock.readLock().lock();
		try {
			for (final Segment segment : segmentMap.values()) {
				segment.optimize();
			}
			
		}
		finally {
			indexLock.readLock().unlock();
		}
	}
	
	public GetNumberOfDocsResponse getNumberOfDocs() throws Exception {
		indexLock.readLock().lock();
		try {
			List<Future<SegmentCountResponse>> responses = new ArrayList<Future<SegmentCountResponse>>();
			
			for (final Segment segment : segmentMap.values()) {
				
				Future<SegmentCountResponse> response = segmentPool.submit(new Callable<SegmentCountResponse>() {
					
					@Override
					public SegmentCountResponse call() throws Exception {
						return segment.getNumberOfDocs();
					}
					
				});
				
				responses.add(response);
				
			}
			
			GetNumberOfDocsResponse.Builder responseBuilder = GetNumberOfDocsResponse.newBuilder();
			
			responseBuilder.setNumberOfDocs(0);
			for (Future<SegmentCountResponse> response : responses) {
				try {
					SegmentCountResponse scr = response.get();
					responseBuilder.addSegmentCountResponse(scr);
					responseBuilder.setNumberOfDocs(responseBuilder.getNumberOfDocs() + scr.getNumberOfDocs());
				}
				catch (InterruptedException e) {
					throw new Exception("Interrupted while waiting for segment results");
				}
				catch (Exception e) {
					Throwable cause = e.getCause();
					if (cause instanceof Exception) {
						throw e;
					}
					
					throw e;
				}
			}
			
			return responseBuilder.build();
		}
		finally {
			indexLock.readLock().unlock();
		}
	}
	
	public GetFieldNamesResponse getFieldNames() throws Exception {
		indexLock.readLock().lock();
		try {
			List<Future<GetFieldNamesResponse>> responses = new ArrayList<Future<GetFieldNamesResponse>>();
			
			for (final Segment segment : segmentMap.values()) {
				
				Future<GetFieldNamesResponse> response = segmentPool.submit(new Callable<GetFieldNamesResponse>() {
					
					@Override
					public GetFieldNamesResponse call() throws Exception {
						return segment.getFieldNames();
					}
					
				});
				
				responses.add(response);
				
			}
			
			GetFieldNamesResponse.Builder responseBuilder = GetFieldNamesResponse.newBuilder();
			
			Set<String> fields = new HashSet<String>();
			for (Future<GetFieldNamesResponse> response : responses) {
				try {
					GetFieldNamesResponse gfnr = response.get();
					fields.addAll(gfnr.getFieldNameList());
				}
				catch (ExecutionException e) {
					Throwable cause = e.getCause();
					if (cause instanceof Exception) {
						throw e;
					}
					else {
						throw new Exception(cause);
					}
				}
			}
			
			responseBuilder.addAllFieldName(fields);
			return responseBuilder.build();
		}
		finally {
			indexLock.readLock().unlock();
		}
	}
	
	public void clear() throws Exception {
		indexLock.writeLock().lock();
		try {
			List<Future<Void>> responses = new ArrayList<Future<Void>>();
			
			for (final Segment segment : segmentMap.values()) {
				
				Future<Void> response = segmentPool.submit(new Callable<Void>() {
					
					@Override
					public Void call() throws Exception {
						segment.clear();
						return null;
					}
					
				});
				
				responses.add(response);
				
			}
			
			for (Future<Void> response : responses) {
				try {
					response.get();
				}
				catch (ExecutionException e) {
					Throwable cause = e.getCause();
					if (cause instanceof Exception) {
						throw e;
					}
					else {
						throw new Exception(cause);
					}
				}
			}
			
		}
		finally {
			indexLock.writeLock().unlock();
		}
	}
	
	public GetTermsResponse getTerms(final GetTermsRequest request) throws Exception {
		indexLock.readLock().lock();
		try {
			List<Future<GetTermsResponse>> responses = new ArrayList<Future<GetTermsResponse>>();
			
			for (final Segment segment : segmentMap.values()) {
				
				Future<GetTermsResponse> response = segmentPool.submit(new Callable<GetTermsResponse>() {
					
					@Override
					public GetTermsResponse call() throws Exception {
						return segment.getTerms(request);
					}
					
				});
				
				responses.add(response);
				
			}
			
			GetTermsResponse.Builder responseBuilder = GetTermsResponse.newBuilder();
			
			TreeSet<String> terms = new TreeSet<String>();
			for (Future<GetTermsResponse> response : responses) {
				try {
					GetTermsResponse gtr = response.get();
					terms.addAll(gtr.getValueList());
				}
				catch (ExecutionException e) {
					Throwable cause = e.getCause();
					if (cause instanceof Exception) {
						throw e;
					}
					else {
						throw new Exception(cause);
					}
				}
			}
			
			int amountToReturn = Math.min(request.getAmount(), terms.size());
			for (int i = 0; i < amountToReturn; i++) {
				String first = terms.first();
				terms.remove(first);
				responseBuilder.addValue(first);
			}
			return responseBuilder.build();
		}
		finally {
			indexLock.readLock().unlock();
		}
	}
	
	private static IndexConfig loadIndexSettings(Mongo mongo, String database, String indexName) throws InvalidIndexConfig {
		DB db = mongo.getDB(database);
		DBCollection dbCollection = db.getCollection(indexName + CONFIG_SUFFIX);
		DBObject settings = dbCollection.findOne(new BasicDBObject(MongoConstants.StandardFields._ID, SETTINGS_ID));
		if (settings == null) {
			throw new InvalidIndexConfig(indexName, "Index settings not found");
		}
		IndexConfig indexConfig = IndexConfig.fromDBObject(settings);
		return indexConfig;
	}
	
	public static Index loadIndex(HazelcastManager hazelcastManager, MongoConfig mongoConfig, ClusterConfig clusterConfig, Mongo mongo, String indexName)
			throws InvalidIndexConfig {
		IndexConfig indexConfig = loadIndexSettings(mongo, mongoConfig.getDatabaseName(), indexName);
		log.info("Loading index <" + indexName + ">");
		
		Index i = new Index(hazelcastManager, mongoConfig, clusterConfig, mongo, indexConfig);
		return i;
		
	}
	
	public static Index createIndex(HazelcastManager hazelcastManager, MongoConfig mongoConfig, ClusterConfig clusterConfig, Mongo mongo,
			IndexConfig indexConfig) {
		log.info("Creating index <" + indexConfig.getIndexName() + ">");
		Index i = new Index(hazelcastManager, mongoConfig, clusterConfig, mongo, indexConfig);
		i.storeIndexSettings();
		return i;
		
	}
	
}
