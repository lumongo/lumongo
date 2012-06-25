package org.lumongo.server.config;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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
	public static final String SEGMENT_TOLERANCE = "segmentTolerance";
	public static final String DEFAULT_ANALYZER = "defaultAnalyzer";
	public static final String FIELD_CONFIGS = "fieldConfigs";
	public static final String FIELD_NAME = "fieldName";
	public static final String ANALYZER = "analyzer";
	public static final String FACETED = "faceted";
	
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
	private boolean blockCompression;
	private double segmentTolerance;
	private LMAnalyzer defaultAnalyzer;
	private List<FieldConfig> fieldConfigList;
	private boolean faceted;
	
	//final because clear is used instead of new HashSet
	private final HashSet<String> numericIntFields;
	private final HashSet<String> numericLongFields;
	private final HashSet<String> numericFloatFields;
	private final HashSet<String> numericDoubleFields;
	private final HashSet<String> numericFields;
	
	protected IndexConfig() {
		this.numericIntFields = new HashSet<String>();
		this.numericLongFields = new HashSet<String>();
		this.numericFloatFields = new HashSet<String>();
		this.numericDoubleFields = new HashSet<String>();
		this.numericFields = new HashSet<String>();
	}
	
	public IndexConfig(IndexCreateRequest request) {
		this();
		
		indexName = request.getIndexName();
		numberOfSegments = request.getNumberOfSegments();
		uniqueIdField = request.getUniqueIdField();
		faceted = request.getFaceted();
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
		this.fieldConfigList = indexSettings.getFieldConfigList();
		
		setupNonNumericFields();
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
		isb.addAllFieldConfig(fieldConfigList);
		isb.setSegmentFlushInterval(segmentFlushInterval);
		return isb.build();
	}
	
	private void setupNonNumericFields() {
		numericIntFields.clear();
		numericLongFields.clear();
		numericFloatFields.clear();
		numericDoubleFields.clear();
		numericFields.clear();
		
		for (FieldConfig fc : fieldConfigList) {
			if (LMAnalyzer.NUMERIC_INT.equals(fc.getAnalyzer())) {
				numericIntFields.add(fc.getFieldName());
			}
			else if (LMAnalyzer.NUMERIC_LONG.equals(fc.getAnalyzer())) {
				numericLongFields.add(fc.getFieldName());
			}
			else if (LMAnalyzer.NUMERIC_FLOAT.equals(fc.getAnalyzer())) {
				numericFloatFields.add(fc.getFieldName());
			}
			else if (LMAnalyzer.NUMERIC_DOUBLE.equals(fc.getAnalyzer())) {
				numericDoubleFields.add(fc.getFieldName());
			}
		}
		
		numericFields.addAll(numericIntFields);
		numericFields.addAll(numericLongFields);
		numericFields.addAll(numericFloatFields);
		numericFields.addAll(numericDoubleFields);
	}
	
	public boolean isNumericField(String fieldName) {
		return numericFields.contains(fieldName);
	}
	
	public boolean isNumericIntField(String fieldName) {
		return numericIntFields.contains(fieldName);
	}
	
	public boolean isNumericLongField(String fieldName) {
		return numericLongFields.contains(fieldName);
	}
	
	public boolean isNumericFloatField(String fieldName) {
		return numericFloatFields.contains(fieldName);
	}
	
	public boolean isNumericDoubleField(String fieldName) {
		return numericDoubleFields.contains(fieldName);
	}
	
	public List<FieldConfig> getFieldConfigList() {
		return fieldConfigList;
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
		
		List<DBObject> fieldConfigs = new ArrayList<DBObject>();
		for (FieldConfig fc : fieldConfigList) {
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
		indexConfig.fieldConfigList = new ArrayList<FieldConfig>();
		
		indexConfig.faceted = false;
		if (settings.containsField(FACETED)) {
			indexConfig.faceted = (boolean) settings.get(FACETED);
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
			indexConfig.fieldConfigList.add(FieldConfig.newBuilder().setFieldName(fieldName).setAnalyzer(analyzer).build());
		}
		
		indexConfig.setupNonNumericFields();
		
		return indexConfig;
	}
	
	@Override
	public String toString() {
		return "IndexConfig [defaultSearchField=" + defaultSearchField + ", applyUncommitedDeletes=" + applyUncommitedDeletes + ", requestFactor="
				+ requestFactor + ", minSegmentRequest=" + minSegmentRequest + ", numberOfSegments=" + numberOfSegments + ", indexName=" + indexName
				+ ", uniqueIdField=" + uniqueIdField + ", idleTimeWithoutCommit=" + idleTimeWithoutCommit + ", segmentFlushInterval=" + segmentFlushInterval
				+ ", segmentCommitInterval=" + segmentCommitInterval + ", blockCompression=" + blockCompression + ", segmentTolerance=" + segmentTolerance
				+ ", defaultAnalyzer=" + defaultAnalyzer + ", fieldConfigList=" + fieldConfigList + ", faceted=" + faceted + ", numericIntFields="
				+ numericIntFields + ", numericLongFields=" + numericLongFields + ", numericFloatFields=" + numericFloatFields + ", numericDoubleFields="
				+ numericDoubleFields + ", numericFields=" + numericFields + "]";
	}
	
}
