package org.lumongo.server.config;

import org.lumongo.DefaultAnalyzers;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.AnalyzerSettings.Filter;
import org.lumongo.cluster.message.Lumongo.AnalyzerSettings.Tokenizer;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.IndexAs;
import org.lumongo.cluster.message.Lumongo.IndexCreateRequest;
import org.lumongo.cluster.message.Lumongo.IndexSettings;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class IndexConfig {

	private int numberOfSegments;
	private String indexName;
	private IndexSettings indexSettings;

	private ConcurrentHashMap<String, FieldConfig> fieldConfigMap;
	private ConcurrentHashMap<String, Lumongo.IndexAs> indexAsMap;
	private ConcurrentHashMap<String, FieldConfig.FieldType> indexFieldType;
	private ConcurrentHashMap<String, FieldConfig.FieldType> sortFieldType;
	private ConcurrentHashMap<String, Lumongo.AnalyzerSettings> analyzerMap;

	public IndexConfig(IndexCreateRequest request) {
		this(request.getIndexName(), request.getNumberOfSegments(), request.getIndexSettings());
	}

	public IndexConfig(String indexName, int numberOfSegments, IndexSettings indexSettings) {
		this.indexName = indexName;
		this.numberOfSegments = numberOfSegments;
		configure(indexSettings);
	}

	public void configure(IndexSettings indexSettings) {
		this.indexSettings = indexSettings;

		this.analyzerMap = new ConcurrentHashMap<>();

		analyzerMap.put(DefaultAnalyzers.STANDARD, Lumongo.AnalyzerSettings.newBuilder().addFilter(Filter.LOWERCASE).addFilter(Filter.STOPWORDS).build());
		analyzerMap.put(DefaultAnalyzers.KEYWORD, Lumongo.AnalyzerSettings.newBuilder().setTokenizer(Tokenizer.KEYWORD).build());
		analyzerMap.put(DefaultAnalyzers.LC_KEYWORD, Lumongo.AnalyzerSettings.newBuilder().setTokenizer(Tokenizer.KEYWORD).addFilter(Filter.LOWERCASE).build());

		for (Lumongo.AnalyzerSettings analyzerSettings : indexSettings.getAnalyzerSettingsList()) {
			analyzerMap.put(analyzerSettings.getName(), analyzerSettings);
		}

		this.fieldConfigMap = new ConcurrentHashMap<>();
		for (FieldConfig fc : indexSettings.getFieldConfigList()) {
			fieldConfigMap.put(fc.getStoredFieldName(), fc);
		}

		this.indexAsMap = new ConcurrentHashMap<>();
		for (String storedFieldName : fieldConfigMap.keySet()) {
			FieldConfig fc = fieldConfigMap.get(storedFieldName);
			for (IndexAs indexAs : fc.getIndexAsList()) {
				indexAsMap.put(indexAs.getIndexFieldName(), indexAs);
			}
		}

		this.indexFieldType = new ConcurrentHashMap<>();
		for (String storedFieldName : fieldConfigMap.keySet()) {
			FieldConfig fc = fieldConfigMap.get(storedFieldName);
			for (IndexAs indexAs : fc.getIndexAsList()) {
				indexFieldType.put(indexAs.getIndexFieldName(), fc.getFieldType());
			}
		}

		this.sortFieldType = new ConcurrentHashMap<>();
		for (String storedFieldName : fieldConfigMap.keySet()) {
			FieldConfig fc = fieldConfigMap.get(storedFieldName);
			for (Lumongo.SortAs sortAs : fc.getSortAsList()) {
				sortFieldType.put(sortAs.getSortFieldName(), fc.getFieldType());
			}
		}

	}

	public IndexSettings getIndexSettings() {
		return indexSettings;
	}

	public Lumongo.AnalyzerSettings getAnalyzerSettingsForIndexField(String fieldName) {
		IndexAs indexAs = indexAsMap.get(fieldName);
		if (indexAs != null) {

			String textAnalyzerName = indexAs.getAnalyzerName();
			return analyzerMap.get(textAnalyzerName);
		}
		return null;
	}

	public FieldConfig.FieldType getFieldTypeForIndexField(String fieldName) {
		return indexFieldType.get(fieldName);
	}

	public FieldConfig.FieldType getFieldTypeForSortField(String sortField) {
		return sortFieldType.get(sortField);
	}

	public Collection<IndexAs> getIndexAsValues() {
		return indexAsMap.values();
	}

	public FieldConfig getFieldConfig(String storedFieldName) {
		return fieldConfigMap.get(storedFieldName);
	}

	public Set<String> getIndexedStoredFieldNames() {
		return fieldConfigMap.keySet();
	}

	public int getNumberOfSegments() {
		return numberOfSegments;
	}

	public String getIndexName() {
		return indexName;
	}

	@Override
	public String toString() {
		return "IndexConfig{" +
				"numberOfSegments=" + numberOfSegments +
				", indexName='" + indexName + '\'' +
				", indexSettings=" + indexSettings +
				'}';
	}
}
