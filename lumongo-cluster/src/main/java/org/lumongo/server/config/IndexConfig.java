package org.lumongo.server.config;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.IndexCreateRequest;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class IndexConfig {
	
	public static final String DEFAULT_SEARCH_FIELD = "defaultSearchField";
	public static final String APPLY_UNCOMMITED_DELETES = "applyUncommitedDeletes";
	public static final String BLOCK_COMPRESSION = "blockCompression";
	public static final String REQUEST_FACTOR = "requestFactor";
	public static final String MIN_SEGMENT_REQUEST = "minSegmentRequest";
	public static final String NUMBER_OF_SEGMENTS = "numberOfSegments";
	public static final String INDEX_NAME = "indexName";
	public static final String UNIQUE_ID_FIELD = "uniqueIdField";
	public static final String IDLE_TIME_WITHOUT_COMMIT = "idleTimeWithoutCommit";
	public static final String SEGMENT_FLUSH_INTERVAL = "segmentFlushInterval";
	public static final String SEGMENT_COMMIT_INTERVAL = "segmentCommitInterval";
	public static final String SEGMENT_QUERY_CACHE_SIZE = "segmentQueryCacheSize";
	public static final String SEGMENT_QUERY_CACHE_MAX_AMOUNT = "segmentQueryCacheMaxAmount";
	public static final String SEGMENT_TOLERANCE = "segmentTolerance";
	public static final String DEFAULT_ANALYZER = "defaultAnalyzer";
	public static final String FIELD_CONFIGS = "fieldConfigs";
	public static final String FIELD_NAME = "fieldName";
	public static final String ANALYZER = "analyzer";
	public static final String FACETED = "faceted";
	public static final String DATABASE_PER_RAW_DOCUMENT_SEGMENT = "databasePerRawDocumentSegment";
	public static final String COLLECTION_PER_RAW_DOCUMENT_SEGMENT = "collectionPerRawDocumentSegment";
	public static final String DATABASE_PER_INDEX_SEGMENT = "databasePerIndexSegment";
	
	private String defaultSearchField;
	private boolean applyUncommitedDeletes;
	private double requestFactor;
	private int minSegmentRequest;
	private int numberOfSegments;
	private String indexName;
	private String uniqueIdField;
	private int idleTimeWithoutCommit;
	private int segmentFlushInterval;
	private int segmentCommitInterval;
	private int segmentQueryCacheSize;
	private int segmentQueryCacheMaxAmount;
	
	private boolean blockCompression;
	private double segmentTolerance;
	private LMAnalyzer defaultAnalyzer;
	private TreeMap<String, FieldConfig> fieldConfigMap;
	private boolean faceted;
	
	private boolean databasePerIndexSegment;
	private boolean collectionPerRawDocumentSegment;
	private boolean databasePerRawDocumentSegment;
	
	protected IndexConfig() {
		
	}
	
	public IndexConfig(IndexCreateRequest request) {
		this();
		
		indexName = request.getIndexName();
		numberOfSegments = request.getNumberOfSegments();
		uniqueIdField = request.getUniqueIdField();
		faceted = request.getFaceted();
		databasePerIndexSegment = request.getDatabasePerIndexSegment();
		collectionPerRawDocumentSegment = request.getCollectionPerRawDocumentSegment();
		databasePerRawDocumentSegment = request.getDatabasePerRawDocumentSegment();
		
		configure(request.getIndexSettings());
	}
	
	public void configure(IndexSettings indexSettings) {
		this.defaultSearchField = indexSettings.getDefaultSearchField();
		this.applyUncommitedDeletes = indexSettings.getApplyUncommitedDeletes();
		this.requestFactor = indexSettings.getRequestFactor();
		this.minSegmentRequest = indexSettings.getMinSegmentRequest();
		this.blockCompression = indexSettings.getBlockCompression();
		this.segmentCommitInterval = indexSettings.getSegmentCommitInterval();
		this.segmentFlushInterval = indexSettings.getSegmentFlushInterval();
		this.idleTimeWithoutCommit = indexSettings.getIdleTimeWithoutCommit();
		this.segmentTolerance = indexSettings.getSegmentTolerance();
		this.defaultAnalyzer = indexSettings.getDefaultAnalyzer();
		this.segmentQueryCacheSize = indexSettings.getSegmentQueryCacheSize();
		this.segmentQueryCacheMaxAmount = indexSettings.getSegmentQueryCacheMaxAmount();
		
		this.fieldConfigMap = new TreeMap<String, FieldConfig>();
		
		for (FieldConfig fc : indexSettings.getFieldConfigList()) {
			fieldConfigMap.put(fc.getFieldName(), fc);
		}
		
	}
	
	public IndexSettings getIndexSettings() {
		IndexSettings.Builder isb = IndexSettings.newBuilder();
		isb.setDefaultSearchField(defaultSearchField);
		isb.setApplyUncommitedDeletes(applyUncommitedDeletes);
		isb.setRequestFactor(requestFactor);
		isb.setMinSegmentRequest(minSegmentRequest);
		isb.setBlockCompression(blockCompression);
		isb.setSegmentCommitInterval(segmentCommitInterval);
		isb.setIdleTimeWithoutCommit(idleTimeWithoutCommit);
		isb.setSegmentTolerance(segmentTolerance);
		isb.setDefaultAnalyzer(defaultAnalyzer);
		isb.addAllFieldConfig(fieldConfigMap.values());
		isb.setSegmentFlushInterval(segmentFlushInterval);
		isb.setSegmentQueryCacheSize(segmentQueryCacheSize);
		isb.setSegmentQueryCacheMaxAmount(segmentQueryCacheMaxAmount);
		return isb.build();
	}
	
	public boolean isNumericField(String fieldName) {
		return isNumericIntField(fieldName) || isNumericLongField(fieldName) || isNumericFloatField(fieldName) || isNumericDoubleField(fieldName);
	}
	
	public boolean isNumericIntField(String fieldName) {
		return LMAnalyzer.NUMERIC_INT.equals(getAnalyzer(fieldName));
	}
	
	public boolean isNumericLongField(String fieldName) {
		return LMAnalyzer.NUMERIC_LONG.equals(getAnalyzer(fieldName));
	}
	
	public boolean isNumericFloatField(String fieldName) {
		return LMAnalyzer.NUMERIC_FLOAT.equals(getAnalyzer(fieldName));
	}
	
	public boolean isNumericDoubleField(String fieldName) {
		return LMAnalyzer.NUMERIC_DOUBLE.equals(getAnalyzer(fieldName));
	}
	
	public LMAnalyzer getAnalyzer(String fieldName) {
		FieldConfig fc = fieldConfigMap.get(fieldName);
		if (fc != null) {
			return fc.getAnalyzer();
		}
		return getDefaultAnalyzer();
	}
	
	public Collection<FieldConfig> getFieldConfigList() {
		return fieldConfigMap.values();
	}
	
	public LMAnalyzer getDefaultAnalyzer() {
		return defaultAnalyzer;
	}
	
	public String getDefaultSearchField() {
		return defaultSearchField;
	}
	
	public boolean getApplyUncommitedDeletes() {
		return applyUncommitedDeletes;
	}
	
	public double getRequestFactor() {
		return requestFactor;
	}
	
	public int getMinSegmentRequest() {
		return minSegmentRequest;
	}
	
	public int getNumberOfSegments() {
		return numberOfSegments;
	}
	
	public String getIndexName() {
		return indexName;
	}
	
	public String getUniqueIdField() {
		return uniqueIdField;
	}
	
	public int getIdleTimeWithoutCommit() {
		return idleTimeWithoutCommit;
	}
	
	public int getSegmentCommitInterval() {
		return segmentCommitInterval;
	}
	
	public int getSegmentFlushInterval() {
		return segmentFlushInterval;
	}
	
	public boolean isBlockCompression() {
		return blockCompression;
	}
	
	public double getSegmentTolerance() {
		return segmentTolerance;
	}
	
	public boolean isFaceted() {
		return faceted;
	}
	
	public boolean isDatabasePerIndexSegment() {
		return databasePerIndexSegment;
	}
	
	public boolean isCollectionPerRawDocumentSegment() {
		return collectionPerRawDocumentSegment;
	}
	
	public boolean isDatabasePerRawDocumentSegment() {
		return databasePerRawDocumentSegment;
	}
	
	public int getSegmentQueryCacheSize() {
		return segmentQueryCacheSize;
	}
		
	public int getSegmentQueryCacheMaxAmount() {
		return segmentQueryCacheMaxAmount;
	}

	public DBObject toDBObject() {
		DBObject dbObject = new BasicDBObject();
		dbObject.put(DEFAULT_SEARCH_FIELD, defaultSearchField);
		dbObject.put(APPLY_UNCOMMITED_DELETES, applyUncommitedDeletes);
		dbObject.put(REQUEST_FACTOR, requestFactor);
		dbObject.put(MIN_SEGMENT_REQUEST, minSegmentRequest);
		dbObject.put(NUMBER_OF_SEGMENTS, numberOfSegments);
		dbObject.put(INDEX_NAME, indexName);
		dbObject.put(UNIQUE_ID_FIELD, uniqueIdField);
		dbObject.put(IDLE_TIME_WITHOUT_COMMIT, idleTimeWithoutCommit);
		dbObject.put(SEGMENT_COMMIT_INTERVAL, segmentCommitInterval);
		dbObject.put(BLOCK_COMPRESSION, blockCompression);
		dbObject.put(SEGMENT_TOLERANCE, segmentTolerance);
		dbObject.put(DEFAULT_ANALYZER, defaultAnalyzer.toString());
		dbObject.put(SEGMENT_FLUSH_INTERVAL, segmentFlushInterval);
		dbObject.put(FACETED, faceted);
		dbObject.put(DATABASE_PER_INDEX_SEGMENT, databasePerIndexSegment);
		dbObject.put(COLLECTION_PER_RAW_DOCUMENT_SEGMENT, collectionPerRawDocumentSegment);
		dbObject.put(DATABASE_PER_RAW_DOCUMENT_SEGMENT, databasePerRawDocumentSegment);
		dbObject.put(SEGMENT_QUERY_CACHE_SIZE, segmentQueryCacheSize);
		dbObject.put(SEGMENT_QUERY_CACHE_MAX_AMOUNT, segmentQueryCacheMaxAmount);
		
		List<DBObject> fieldConfigs = new ArrayList<DBObject>();
		for (FieldConfig fc : fieldConfigMap.values()) {
			BasicDBObject fieldConfig = new BasicDBObject();
			fieldConfig.put(FIELD_NAME, fc.getFieldName());
			fieldConfig.put(ANALYZER, fc.getAnalyzer().toString());
			fieldConfigs.add(fieldConfig);
		}
		
		dbObject.put(FIELD_CONFIGS, fieldConfigs);
		
		return dbObject;
		
	}
	
	@SuppressWarnings("unchecked")
	public static IndexConfig fromDBObject(DBObject settings) {
		IndexConfig indexConfig = new IndexConfig();
		indexConfig.defaultSearchField = (String) settings.get(DEFAULT_SEARCH_FIELD);
		indexConfig.applyUncommitedDeletes = (boolean) settings.get(APPLY_UNCOMMITED_DELETES);
		indexConfig.requestFactor = (double) settings.get(REQUEST_FACTOR);
		indexConfig.minSegmentRequest = (int) settings.get(MIN_SEGMENT_REQUEST);
		indexConfig.numberOfSegments = (int) settings.get(NUMBER_OF_SEGMENTS);
		indexConfig.indexName = (String) settings.get(INDEX_NAME);
		indexConfig.uniqueIdField = (String) settings.get(UNIQUE_ID_FIELD);
		indexConfig.idleTimeWithoutCommit = (int) settings.get(IDLE_TIME_WITHOUT_COMMIT);
		indexConfig.segmentCommitInterval = (int) settings.get(SEGMENT_COMMIT_INTERVAL);
		indexConfig.blockCompression = (boolean) settings.get(BLOCK_COMPRESSION);
		indexConfig.segmentTolerance = (double) settings.get(SEGMENT_TOLERANCE);
		indexConfig.defaultAnalyzer = LMAnalyzer.valueOf((String) settings.get(DEFAULT_ANALYZER));
		
		if (settings.get(SEGMENT_QUERY_CACHE_SIZE) != null) {
			indexConfig.segmentQueryCacheSize = (int) settings.get(SEGMENT_QUERY_CACHE_SIZE);
		}
		if (settings.get(SEGMENT_QUERY_CACHE_MAX_AMOUNT) != null) {
			indexConfig.segmentQueryCacheMaxAmount = (int) settings.get(SEGMENT_QUERY_CACHE_MAX_AMOUNT);
		}
		
		indexConfig.fieldConfigMap = new TreeMap<String, FieldConfig>();
		
		indexConfig.faceted = false;
		if (settings.containsField(FACETED)) {
			indexConfig.faceted = (boolean) settings.get(FACETED);
		}
		
		if (settings.containsField(DATABASE_PER_INDEX_SEGMENT)) {
			indexConfig.databasePerIndexSegment = (boolean) settings.get(DATABASE_PER_INDEX_SEGMENT);
		}
		if (settings.containsField(COLLECTION_PER_RAW_DOCUMENT_SEGMENT)) {
			indexConfig.collectionPerRawDocumentSegment = (boolean) settings.get(COLLECTION_PER_RAW_DOCUMENT_SEGMENT);
		}
		if (settings.containsField(DATABASE_PER_RAW_DOCUMENT_SEGMENT)) {
			indexConfig.databasePerRawDocumentSegment = (boolean) settings.get(DATABASE_PER_RAW_DOCUMENT_SEGMENT);
		}
		
		if (settings.containsField(SEGMENT_FLUSH_INTERVAL)) {
			indexConfig.segmentFlushInterval = (int) settings.get(SEGMENT_FLUSH_INTERVAL);
		}
		else {
			//make flush interval equal to segment commit interval divided by 2 if not defined (for old indexes)
			indexConfig.segmentFlushInterval = (indexConfig.segmentCommitInterval / 2);
		}
		
		List<DBObject> fieldConfigs = (List<DBObject>) settings.get(FIELD_CONFIGS);
		for (DBObject fieldConfig : fieldConfigs) {
			String fieldName = (String) fieldConfig.get(FIELD_NAME);
			LMAnalyzer analyzer = LMAnalyzer.valueOf((String) fieldConfig.get(ANALYZER));
			indexConfig.fieldConfigMap.put(fieldName, FieldConfig.newBuilder().setFieldName(fieldName).setAnalyzer(analyzer).build());
		}
		
		return indexConfig;
	}

	@Override
	public String toString() {
		return "IndexConfig [defaultSearchField=" + defaultSearchField
				+ ", applyUncommitedDeletes=" + applyUncommitedDeletes
				+ ", requestFactor=" + requestFactor + ", minSegmentRequest="
				+ minSegmentRequest + ", numberOfSegments=" + numberOfSegments
				+ ", indexName=" + indexName + ", uniqueIdField="
				+ uniqueIdField + ", idleTimeWithoutCommit="
				+ idleTimeWithoutCommit + ", segmentFlushInterval="
				+ segmentFlushInterval + ", segmentCommitInterval="
				+ segmentCommitInterval + ", segmentQueryCacheSize="
				+ segmentQueryCacheSize + ", segmentQueryCacheMaxAmount="
				+ segmentQueryCacheMaxAmount + ", blockCompression="
				+ blockCompression + ", segmentTolerance=" + segmentTolerance
				+ ", defaultAnalyzer=" + defaultAnalyzer + ", fieldConfigMap="
				+ fieldConfigMap + ", faceted=" + faceted
				+ ", databasePerIndexSegment=" + databasePerIndexSegment
				+ ", collectionPerRawDocumentSegment="
				+ collectionPerRawDocumentSegment
				+ ", databasePerRawDocumentSegment="
				+ databasePerRawDocumentSegment + "]";
	}
	

	
}
