package org.lumongo.client.config;

import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.fields.FieldConfigBuilder;

import java.util.TreeMap;

public class IndexConfig {
	
	private String defaultSearchField;
	private Boolean applyUncommitedDeletes;
	private Double requestFactor;
	private Integer minSegmentRequest;
	private Integer numberOfSegments;
	private String indexName;
	private Integer idleTimeWithoutCommit;
	private Integer segmentFlushInterval;
	private Integer segmentCommitInterval;
	private Boolean blockCompression;
	private Double segmentTolerance;
	private Integer segmentQueryCacheSize;
	private Integer segmentQueryCacheMaxAmount;
	
	private TreeMap<String, FieldConfig> fieldMap;
	
	protected IndexConfig() {
		
	}
	
	public IndexConfig(String defaultSearchField) {
		this.defaultSearchField = defaultSearchField;
		this.fieldMap = new TreeMap<String, FieldConfig>();
	}
	
	public String getDefaultSearchField() {
		return defaultSearchField;
	}
	
	public IndexConfig setDefaultSearchField(String defaultSearchField) {
		this.defaultSearchField = defaultSearchField;
		return this;
	}
	
	public boolean isApplyUncommitedDeletes() {
		return applyUncommitedDeletes;
	}
	
	public IndexConfig setApplyUncommitedDeletes(boolean applyUncommitedDeletes) {
		this.applyUncommitedDeletes = applyUncommitedDeletes;
		return this;
	}
	
	public double getRequestFactor() {
		return requestFactor;
	}
	
	public IndexConfig setRequestFactor(double requestFactor) {
		this.requestFactor = requestFactor;
		return this;
	}
	
	public int getMinSegmentRequest() {
		return minSegmentRequest;
	}
	
	public IndexConfig setMinSegmentRequest(int minSegmentRequest) {
		this.minSegmentRequest = minSegmentRequest;
		return this;
	}
	
	public int getNumberOfSegments() {
		return numberOfSegments;
	}
	
	public IndexConfig setNumberOfSegments(int numberOfSegments) {
		this.numberOfSegments = numberOfSegments;
		return this;
	}
	
	public String getIndexName() {
		return indexName;
	}
	
	public IndexConfig setIndexName(String indexName) {
		this.indexName = indexName;
		return this;
	}
	
	public int getIdleTimeWithoutCommit() {
		return idleTimeWithoutCommit;
	}
	
	public IndexConfig setIdleTimeWithoutCommit(int idleTimeWithoutCommit) {
		this.idleTimeWithoutCommit = idleTimeWithoutCommit;
		return this;
	}
	
	public int getSegmentFlushInterval() {
		return segmentFlushInterval;
	}
	
	public IndexConfig setSegmentFlushInterval(int segmentFlushInterval) {
		this.segmentFlushInterval = segmentFlushInterval;
		return this;
	}
	
	public int getSegmentCommitInterval() {
		return segmentCommitInterval;
	}
	
	public IndexConfig setSegmentCommitInterval(int segmentCommitInterval) {
		this.segmentCommitInterval = segmentCommitInterval;
		return this;
	}
	
	public boolean isBlockCompression() {
		return blockCompression;
	}
	
	public IndexConfig setBlockCompression(boolean blockCompression) {
		this.blockCompression = blockCompression;
		return this;
	}
	
	public double getSegmentTolerance() {
		return segmentTolerance;
	}
	
	public IndexConfig setSegmentTolerance(double segmentTolerance) {
		this.segmentTolerance = segmentTolerance;
		return this;
	}
	
	public Integer getSegmentQueryCacheSize() {
		return segmentQueryCacheSize;
	}
	
	public void setSegmentQueryCacheSize(Integer segmentQueryCacheSize) {
		this.segmentQueryCacheSize = segmentQueryCacheSize;
	}
	
	public Integer getSegmentQueryCacheMaxAmount() {
		return segmentQueryCacheMaxAmount;
	}
	
	public void setSegmentQueryCacheMaxAmount(Integer segmentQueryCacheMaxAmount) {
		this.segmentQueryCacheMaxAmount = segmentQueryCacheMaxAmount;
	}
	
	public void addFieldConfig(FieldConfigBuilder FieldConfigBuilder) {
		addFieldConfig(FieldConfigBuilder.build());
	}
	
	public void addFieldConfig(FieldConfig fieldConfig) {
		this.fieldMap.put(fieldConfig.getStoredFieldName(), fieldConfig);
	}
	
	public FieldConfig getFieldConfig(String fieldName) {
		return this.fieldMap.get(fieldName);
	}

	public TreeMap<String, FieldConfig> getFieldConfigMap() {
		return fieldMap;
	}

	public IndexSettings getIndexSettings() {
		IndexSettings.Builder isb = IndexSettings.newBuilder();
		if (defaultSearchField != null) {
			isb.setDefaultSearchField(defaultSearchField);
		}
		if (applyUncommitedDeletes != null) {
			isb.setApplyUncommitedDeletes(applyUncommitedDeletes);
		}
		if (requestFactor != null) {
			isb.setRequestFactor(requestFactor);
		}
		if (minSegmentRequest != null) {
			isb.setMinSegmentRequest(minSegmentRequest);
		}
		if (blockCompression != null) {
			isb.setBlockCompression(blockCompression);
		}
		if (segmentCommitInterval != null) {
			isb.setSegmentCommitInterval(segmentCommitInterval);
		}
		if (idleTimeWithoutCommit != null) {
			isb.setIdleTimeWithoutCommit(idleTimeWithoutCommit);
		}
		if (segmentTolerance != null) {
			isb.setSegmentTolerance(segmentTolerance);
		}
		if (segmentFlushInterval != null) {
			isb.setSegmentFlushInterval(segmentFlushInterval);
		}
		if (segmentQueryCacheSize != null) {
			isb.setSegmentQueryCacheSize(segmentQueryCacheSize);
		}
		
		if (segmentQueryCacheMaxAmount != null) {
			isb.setSegmentQueryCacheMaxAmount(segmentQueryCacheMaxAmount);
		}
		
		for (String fieldName : fieldMap.keySet()) {
			FieldConfig fieldConfig = fieldMap.get(fieldName);
			isb.addFieldConfig(fieldConfig);
		}
		
		return isb.build();
	}
	
	protected void configure(IndexSettings indexSettings) {
		this.defaultSearchField = indexSettings.getDefaultSearchField();
		this.applyUncommitedDeletes = indexSettings.getApplyUncommitedDeletes();
		this.requestFactor = indexSettings.getRequestFactor();
		this.minSegmentRequest = indexSettings.getMinSegmentRequest();
		this.blockCompression = indexSettings.getBlockCompression();
		this.segmentCommitInterval = indexSettings.getSegmentCommitInterval();
		this.segmentFlushInterval = indexSettings.getSegmentFlushInterval();
		this.idleTimeWithoutCommit = indexSettings.getIdleTimeWithoutCommit();
		this.segmentTolerance = indexSettings.getSegmentTolerance();
		this.segmentQueryCacheSize = indexSettings.getSegmentQueryCacheSize();
		this.segmentQueryCacheMaxAmount = indexSettings.getSegmentQueryCacheMaxAmount();
		this.fieldMap = new TreeMap<>();
		
		for (FieldConfig fc : indexSettings.getFieldConfigList()) {
			fieldMap.put(fc.getStoredFieldName(), fc);
		}
		
	}
	
	public static IndexConfig fromIndexSettings(IndexSettings indexSettings) {
		IndexConfig ic = new IndexConfig();
		ic.configure(indexSettings);
		return ic;
	}
	
}
