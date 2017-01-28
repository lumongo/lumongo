package org.lumongo.server.config;

import org.bson.Document;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.AnalyzerSettings;
import org.lumongo.cluster.message.Lumongo.FacetAs;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.IndexAs;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.cluster.message.Lumongo.SortAs;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by mdavis on 4/24/16.
 */
public class IndexConfigUtil {

	private static final String DEFAULT_SEARCH_FIELD = "defaultSearchField";
	private static final String APPLY_UNCOMMITTED_DELETES = "applyUncommittedDeletes";
	private static final String REQUEST_FACTOR = "requestFactor";
	private static final String MIN_SEGMENT_REQUEST = "minSegmentRequest";
	private static final String NUMBER_OF_SEGMENTS = "numberOfSegments";
	private static final String INDEX_NAME = "indexName";
	private static final String IDLE_TIME_WITHOUT_COMMIT = "idleTimeWithoutCommit";
	private static final String SEGMENT_COMMIT_INTERVAL = "segmentCommitInterval";
	private static final String SEGMENT_QUERY_CACHE_SIZE = "segmentQueryCacheSize";
	private static final String SEGMENT_QUERY_CACHE_MAX_AMOUNT = "segmentQueryCacheMaxAmount";
	private static final String STORE_DOCUMENT_IN_MONGO = "storeDocumentInMongo";
	private static final String STORE_DOCUMENT_IN_INDEX = "storeDocumentInIndex";
	private static final String STORE_INDEX_ON_DISK = "storeIndexOnDisk";
	private static final String SEGMENT_TOLERANCE = "segmentTolerance";
	private static final String FIELD_CONFIGS = "fieldConfigs";
	private static final String STORED_FIELD_NAME = "storedFieldName";
	private static final String INDEXED_FIELD_NAME = "indexedFieldName";
	private static final String ANALYZER_NAME = "analyzerName";
	private static final String QUERY_HANDLING = "queryHandling";
	private static final String INDEX_AS = "indexAs";
	private static final String FACET_AS = "facetAs";
	private static final String SORT_AS = "sortAs";
	private static final String PROJECT_AS = "projectAs";
	private static final String FACET_NAME = "facetName";
	private static final String FIELD_TYPE = "fieldType";
	private static final String FACET_DATE_HANDLING = "dateHandling";
	private static final String SORT_STRING_HANDLING = "stringHandling";
	private static final String SORT_FIELD_NAME = "sortFieldName";
	private static final String FIELD = "field";
	private static final String SUPER_BIT = "superBit";
	private static final String INPUT_DIM = "inputDim";
	private static final String BATCHES = "batches";
	private static final String SEED = "seed";

	private static final String TOKENIZER = "tokenizer";
	private static final String SIMILARITY = "similarity";
	private static final String FILTERS = "filters";
	private static final String ANALYZER_SETTINGS = "analyzerSettings";

	public static boolean isNumericIntFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.NUMERIC_INT.equals(fieldType);
	}

	public static boolean isNumericLongFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.NUMERIC_LONG.equals(fieldType);
	}

	public static boolean isNumericFloatFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.NUMERIC_FLOAT.equals(fieldType);
	}

	public static boolean isNumericDoubleFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.NUMERIC_DOUBLE.equals(fieldType);
	}

	public static boolean isDateFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.DATE.equals(fieldType);
	}

	public static boolean isNumericOrDateFieldType(FieldConfig.FieldType fieldType) {
		return isNumericIntFieldType(fieldType) || isNumericLongFieldType(fieldType) || isNumericFloatFieldType(fieldType) || isNumericDoubleFieldType(
				fieldType) || isDateFieldType(fieldType);
	}

	@SuppressWarnings("unchecked")
	public static IndexConfig fromDocument(Document settings) {

		IndexSettings.Builder indexSettings = IndexSettings.newBuilder();

		String indexName = settings.getString(INDEX_NAME);
		int numberOfSegments = settings.getInteger(NUMBER_OF_SEGMENTS);

		indexSettings.setDefaultSearchField(settings.getString(DEFAULT_SEARCH_FIELD));
		indexSettings.setStoreDocumentInMongo(settings.getBoolean(STORE_DOCUMENT_IN_MONGO));
		indexSettings.setStoreDocumentInIndex(settings.getBoolean(STORE_DOCUMENT_IN_INDEX));
		indexSettings.setApplyUncommittedDeletes(settings.getBoolean(APPLY_UNCOMMITTED_DELETES));
		indexSettings.setRequestFactor(settings.getDouble(REQUEST_FACTOR));
		indexSettings.setMinSegmentRequest(settings.getInteger(MIN_SEGMENT_REQUEST));
		indexSettings.setIdleTimeWithoutCommit(settings.getInteger(IDLE_TIME_WITHOUT_COMMIT));
		indexSettings.setSegmentCommitInterval(settings.getInteger(SEGMENT_COMMIT_INTERVAL));
		indexSettings.setSegmentTolerance(settings.getDouble(SEGMENT_TOLERANCE));
		indexSettings.setSegmentQueryCacheSize(settings.getInteger(SEGMENT_QUERY_CACHE_SIZE));
		indexSettings.setSegmentQueryCacheMaxAmount(settings.getInteger(SEGMENT_QUERY_CACHE_MAX_AMOUNT));
		indexSettings.setStoreIndexOnDisk(settings.getBoolean(STORE_INDEX_ON_DISK));

		Document analyzerSettings = settings.get(ANALYZER_SETTINGS, Document.class);
		for (String key : analyzerSettings.keySet()) {
			AnalyzerSettings as = getAnalyzerSettings(analyzerSettings.get(key, Document.class)).setName(key).build();
			indexSettings.addAnalyzerSettings(as);
		}

		List<Document> fieldConfigs = (List<Document>) settings.get(FIELD_CONFIGS);
		for (Document fieldConfigObj : fieldConfigs) {

			FieldConfig.Builder fieldConfig = FieldConfig.newBuilder();

			String storedFieldName = fieldConfigObj.getString(STORED_FIELD_NAME);
			fieldConfig.setStoredFieldName(storedFieldName);

			FieldConfig.FieldType fieldType = FieldConfig.FieldType.valueOf((String) fieldConfigObj.get(FIELD_TYPE));
			fieldConfig.setFieldType(fieldType);

			{
				List<Document> indexAsObjList = (List<Document>) fieldConfigObj.get(INDEX_AS);
				for (Document indexAsObj : indexAsObjList) {

					IndexAs.Builder indexAs = IndexAs.newBuilder();
					indexAs.setIndexFieldName(indexAsObj.getString(INDEXED_FIELD_NAME));
					String analyzerName = indexAsObj.getString(ANALYZER_NAME);
					if (analyzerName != null) {
						indexAs.setAnalyzerName(analyzerName);
					}
					fieldConfig.addIndexAs(indexAs);
				}
			}

			{

				List<Document> facetAsObjList = (List<Document>) fieldConfigObj.get(FACET_AS);
				for (Document facetAsObj : facetAsObjList) {

					FacetAs.Builder facetAs = FacetAs.newBuilder();
					facetAs.setFacetName(facetAsObj.getString(FACET_NAME));
					String dateHandling = facetAsObj.getString(FACET_DATE_HANDLING);
					if (dateHandling != null) {
						facetAs.setDateHandling(FacetAs.DateHandling.valueOf(dateHandling));
					}

					fieldConfig.addFacetAs(facetAs);
				}
			}
			{
				List<Document> sortAsDocList = (List<Document>) fieldConfigObj.get(SORT_AS);
				for (Document sortAsObj : sortAsDocList) {

					SortAs.Builder builder = SortAs.newBuilder();
					builder.setSortFieldName(sortAsObj.getString(SORT_FIELD_NAME));
					String stringHandling = sortAsObj.getString(SORT_STRING_HANDLING);
					if (stringHandling != null) {
						builder.setStringHandling(SortAs.StringHandling.valueOf(stringHandling));
					}

					fieldConfig.addSortAs(builder);
				}
			}
			{
				List<Document> projectAsList = (List<Document>) fieldConfigObj.get(PROJECT_AS);
				if (projectAsList != null) {
					for (Document projectAsObj : projectAsList) {

						Lumongo.ProjectAs.Builder builder = Lumongo.ProjectAs.newBuilder();
						builder.setField(projectAsObj.getString(FIELD));

						Document superBitObj = (Document) projectAsObj.get(SUPER_BIT);
						if (superBitObj != null) {
							Lumongo.Superbit.Builder sbBuilder = Lumongo.Superbit.newBuilder();
							Integer inputDim = superBitObj.getInteger(INPUT_DIM);
							if (inputDim != null) {
								sbBuilder.setInputDim(inputDim);
							}
							Integer batches = superBitObj.getInteger(BATCHES);
							if (batches != null) {
								sbBuilder.setBatches(batches);
							}
							Integer seed = superBitObj.getInteger(SEED);
							if (seed != null) {
								sbBuilder.setSeed(seed);
							}

							builder.setSuperbit(sbBuilder);
						}

						fieldConfig.addProjectAs(builder);
					}
				}
			}

			indexSettings.addFieldConfig(fieldConfig);
		}

		return new IndexConfig(indexName, numberOfSegments, indexSettings.build());
	}

	private static AnalyzerSettings.Builder getAnalyzerSettings(Document analyzerSettingsDoc) {
		AnalyzerSettings.Builder analyzerSettings = AnalyzerSettings.newBuilder();

		String similarity = analyzerSettingsDoc.getString(SIMILARITY);
		if (similarity != null) {
			analyzerSettings.setSimilarity(AnalyzerSettings.Similarity.valueOf(similarity));
		}
		String tokenizer = analyzerSettingsDoc.getString(TOKENIZER);
		if (tokenizer != null) {
			analyzerSettings.setTokenizer(AnalyzerSettings.Tokenizer.valueOf(tokenizer));
		}

		String queryHandling = analyzerSettingsDoc.getString(QUERY_HANDLING);
		if (queryHandling != null) {
			analyzerSettings.setQueryHandling(AnalyzerSettings.QueryHandling.valueOf(queryHandling));
		}

		List<String> filters = (List<String>) analyzerSettingsDoc.get(FILTERS);
		if (filters != null) {
			for (String filter : filters) {
				analyzerSettings.addFilter(AnalyzerSettings.Filter.valueOf(filter));
			}
		}
		return analyzerSettings;
	}

	public static Document toDocument(IndexConfig indexConfig) {
		Document document = new Document();
		document.put(NUMBER_OF_SEGMENTS, indexConfig.getNumberOfSegments());
		document.put(INDEX_NAME, indexConfig.getIndexName());

		IndexSettings indexSettings = indexConfig.getIndexSettings();
		document.put(DEFAULT_SEARCH_FIELD, indexSettings.getDefaultSearchField());
		document.put(STORE_DOCUMENT_IN_MONGO, indexSettings.getStoreDocumentInMongo());
		document.put(STORE_DOCUMENT_IN_INDEX, indexSettings.getStoreDocumentInIndex());
		document.put(STORE_INDEX_ON_DISK, indexSettings.getStoreIndexOnDisk());
		document.put(APPLY_UNCOMMITTED_DELETES, indexSettings.getApplyUncommittedDeletes());
		document.put(REQUEST_FACTOR, indexSettings.getRequestFactor());
		document.put(MIN_SEGMENT_REQUEST, indexSettings.getMinSegmentRequest());
		document.put(IDLE_TIME_WITHOUT_COMMIT, indexSettings.getIdleTimeWithoutCommit());
		document.put(SEGMENT_COMMIT_INTERVAL, indexSettings.getSegmentCommitInterval());
		document.put(SEGMENT_TOLERANCE, indexSettings.getSegmentTolerance());
		document.put(SEGMENT_QUERY_CACHE_SIZE, indexSettings.getSegmentQueryCacheSize());
		document.put(SEGMENT_QUERY_CACHE_MAX_AMOUNT, indexSettings.getSegmentQueryCacheMaxAmount());

		Document analyzerSettingsDocs = new Document();
		for (AnalyzerSettings analyzerSettings : indexSettings.getAnalyzerSettingsList()) {

			Document analyzerSettingsDoc = new Document();
			if (analyzerSettings.hasTokenizer()) {
				analyzerSettingsDoc.put(TOKENIZER, analyzerSettings.getTokenizer().name());
			}
			if (analyzerSettings.hasSimilarity()) {
				analyzerSettingsDoc.put(SIMILARITY, analyzerSettings.getSimilarity().name());
			}
			List<String> filtersList = analyzerSettings.getFilterList().stream().map(AnalyzerSettings.Filter::name).collect(Collectors.toList());
			if (!filtersList.isEmpty()) {
				analyzerSettingsDoc.put(FILTERS, filtersList);
			}
			if (analyzerSettings.hasQueryHandling()) {
				analyzerSettingsDoc.put(QUERY_HANDLING, analyzerSettings.getQueryHandling().name());
			}

			analyzerSettingsDocs.put(analyzerSettings.getName(), analyzerSettingsDoc);
		}
		document.put(ANALYZER_SETTINGS, analyzerSettingsDocs);

		List<Document> fieldConfigs = new ArrayList<>();
		for (FieldConfig fc : indexSettings.getFieldConfigList()) {
			Document fieldConfig = new Document();
			fieldConfig.put(FIELD_TYPE, fc.getFieldType().name());
			fieldConfig.put(STORED_FIELD_NAME, fc.getStoredFieldName());
			{
				List<Document> indexAsObjList = new ArrayList<>();
				for (IndexAs indexAs : fc.getIndexAsList()) {
					Document indexAsObj = new Document();
					indexAsObj.put(INDEXED_FIELD_NAME, indexAs.getIndexFieldName());
					if (indexAs.hasAnalyzerName()) {
						indexAsObj.put(ANALYZER_NAME, indexAs.getAnalyzerName());
					}
					indexAsObjList.add(indexAsObj);
				}
				fieldConfig.put(INDEX_AS, indexAsObjList);
			}
			{
				List<Document> facetAsObjList = new ArrayList<>();
				for (FacetAs facetAs : fc.getFacetAsList()) {
					Document facetAsObj = new Document();
					facetAsObj.put(FACET_NAME, facetAs.getFacetName());
					if (facetAs.hasDateHandling()) {
						facetAsObj.put(FACET_DATE_HANDLING, facetAs.getDateHandling().name());
					}
					facetAsObjList.add(facetAsObj);
				}
				fieldConfig.put(FACET_AS, facetAsObjList);
			}
			{

				List<Document> sortAsObjList = new ArrayList<>();
				for (SortAs sortAs : fc.getSortAsList()) {
					Document sortAsObj = new Document();
					sortAsObj.put(SORT_FIELD_NAME, sortAs.getSortFieldName());
					if (sortAs.hasStringHandling()) {
						sortAsObj.put(SORT_STRING_HANDLING, sortAs.getStringHandling().name());
					}
					sortAsObjList.add(sortAsObj);
				}
				fieldConfig.put(SORT_AS, sortAsObjList);
			}

			{
				List<Document> projectAsObjlist = new ArrayList<>();
				for (Lumongo.ProjectAs projectAs : fc.getProjectAsList()) {
					Document projectAsObj = new Document();
					projectAsObj.put(FIELD, projectAs.getField());
					if (projectAs.hasSuperbit()) {
						Document sbObj = new Document();

						Lumongo.Superbit superbit = projectAs.getSuperbit();
						sbObj.put(INPUT_DIM, superbit.getInputDim());
						sbObj.put(BATCHES, superbit.getBatches());
						sbObj.put(SEED, superbit.getSeed());

						projectAsObj.put(SUPER_BIT, sbObj);
					}
					projectAsObjlist.add(projectAsObj);
				}
				fieldConfig.put(PROJECT_AS, projectAsObjlist);
			}

			fieldConfigs.add(fieldConfig);
		}

		document.put(FIELD_CONFIGS, fieldConfigs);

		return document;

	}

}
