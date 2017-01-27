package org.lumongo.server.config;

import info.debatty.java.lsh.SuperBit;
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
	private ConcurrentHashMap<String, String> indexToStoredMap;
	private ConcurrentHashMap<String, Lumongo.FacetAs> facetAsMap;
	private ConcurrentHashMap<String, Lumongo.Superbit> superbitConfigMap;
	private ConcurrentHashMap<String, SuperBit> superbitMap;

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

		analyzerMap.put(DefaultAnalyzers.STANDARD,
				Lumongo.AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.STANDARD).addFilter(Filter.LOWERCASE).addFilter(Filter.STOPWORDS).build());
		analyzerMap
				.put(DefaultAnalyzers.KEYWORD, Lumongo.AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.KEYWORD).setTokenizer(Tokenizer.KEYWORD).build());
		analyzerMap.put(DefaultAnalyzers.LC_KEYWORD,
				Lumongo.AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.LC_KEYWORD).setTokenizer(Tokenizer.KEYWORD).addFilter(Filter.LOWERCASE).build());
		analyzerMap.put(DefaultAnalyzers.MIN_STEM,
				Lumongo.AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.MIN_STEM).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.STOPWORDS).addFilter(Filter.ENGLISH_MIN_STEM).build());

		analyzerMap.put(DefaultAnalyzers.TWO_TWO_SHINGLE,
				Lumongo.AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.TWO_TWO_SHINGLE).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.TWO_TWO_SHINGLE).build());
		analyzerMap.put(DefaultAnalyzers.THREE_THREE_SHINGLE,
				Lumongo.AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.THREE_THREE_SHINGLE).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.THREE_THREE_SHINGLE).build());

		analyzerMap.put(DefaultAnalyzers.KSTEMMED,
				Lumongo.AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.KSTEMMED).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.STOPWORDS).addFilter(Filter.KSTEM).build());
		analyzerMap.put(DefaultAnalyzers.LSH,
				Lumongo.AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.LSH).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.ASCII_FOLDING).addFilter(Filter.KSTEM).addFilter(Filter.STOPWORDS).addFilter(Filter.FIVE_FIVE_SHINGLE)
						.addFilter(Filter.MINHASH).build());

		for (Lumongo.AnalyzerSettings analyzerSettings : indexSettings.getAnalyzerSettingsList()) {
			analyzerMap.put(analyzerSettings.getName(), analyzerSettings);
		}

		this.fieldConfigMap = new ConcurrentHashMap<>();
		for (FieldConfig fc : indexSettings.getFieldConfigList()) {
			fieldConfigMap.put(fc.getStoredFieldName(), fc);
		}

		this.indexAsMap = new ConcurrentHashMap<>();
		this.indexToStoredMap = new ConcurrentHashMap<>();
		for (String storedFieldName : fieldConfigMap.keySet()) {
			FieldConfig fc = fieldConfigMap.get(storedFieldName);
			for (IndexAs indexAs : fc.getIndexAsList()) {
				indexAsMap.put(indexAs.getIndexFieldName(), indexAs);
				indexToStoredMap.put(indexAs.getIndexFieldName(), storedFieldName);
			}
		}

		this.facetAsMap = new ConcurrentHashMap<>();
		for (String storedFieldName : fieldConfigMap.keySet()) {
			FieldConfig fc = fieldConfigMap.get(storedFieldName);
			for (Lumongo.FacetAs facetAs : fc.getFacetAsList()) {
				facetAsMap.put(facetAs.getFacetName(), facetAs);
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

		this.superbitConfigMap = new ConcurrentHashMap<>();
		this.superbitMap = new ConcurrentHashMap<>();
		for (String storedFieldName : fieldConfigMap.keySet()) {
			FieldConfig fc = fieldConfigMap.get(storedFieldName);
			for (Lumongo.ProjectAs projectAs : fc.getProjectAsList()) {
				String field = projectAs.getField();
				if (projectAs.hasSuperbit()) {
					Lumongo.Superbit superbit = projectAs.getSuperbit();
					superbitConfigMap.put(field, superbit);

					SuperBit superBit = new SuperBit(superbit.getInputDim(), superbit.getInputDim(), superbit.getBatches(), superbit.getSeed());
					superbitMap.put(field, superBit);
				}
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
			return getAnalyzerSettingsByName(textAnalyzerName);
		}
		return null;
	}

	public boolean existingFacet(String facet) {
		return facetAsMap.containsKey(facet);
	}

	public SuperBit getSuperBitForField(String field) {
		return superbitMap.get(field);
	}

	public Lumongo.Superbit getSuperBitConfigForField(String field) {
		return superbitConfigMap.get(field);
	}

	public Lumongo.AnalyzerSettings getAnalyzerSettingsByName(String textAnalyzerName) {
		return analyzerMap.get(textAnalyzerName);
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

	public String getStoredFieldName(String indexFieldName) {
		return indexToStoredMap.get(indexFieldName);
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
		return "IndexConfig{" + "numberOfSegments=" + numberOfSegments + ", indexName='" + indexName + '\'' + ", indexSettings=" + indexSettings + '}';
	}

}
