package org.lumongo.server.index;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LegacyLongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.*;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.InvalidTokenOffsetsException;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.bson.BSON;
import org.bson.BSONObject;
import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.*;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
import org.lumongo.server.highlighter.LumongoHighlighter;
import org.lumongo.util.ResultHelper;
import org.lumongo.server.config.IndexConfig;
import org.lumongo.server.config.IndexConfigUtil;
import org.lumongo.server.index.field.BooleanFieldIndexer;
import org.lumongo.server.index.field.DateFieldIndexer;
import org.lumongo.server.index.field.DoubleFieldIndexer;
import org.lumongo.server.index.field.FloatFieldIndexer;
import org.lumongo.server.index.field.IntFieldIndexer;
import org.lumongo.server.index.field.LongFieldIndexer;
import org.lumongo.server.index.field.StringFieldIndexer;
import org.lumongo.server.search.FacetStateCache;
import org.lumongo.server.search.QueryCacheKey;
import org.lumongo.server.search.QueryResultCache;
import org.lumongo.server.search.QueryWithFilters;
import org.lumongo.server.search.facet.LumongoSortedSetDocValuesFacetCounts;
import org.lumongo.similarity.ConstantSimilarity;
import org.lumongo.similarity.TFSimilarity;
import org.lumongo.storage.rawfiles.DocumentStorage;
import org.lumongo.util.LumongoUtil;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LumongoSegment {

	private final static DateTimeFormatter FORMATTER_YYYYMMDD = DateTimeFormatter.BASIC_ISO_DATE;
	private final static DateTimeFormatter FORMATTER_YYYY_MM_DD = DateTimeFormatter.ISO_DATE;

	private final static Logger log = Logger.getLogger(LumongoSegment.class);
	private static Pattern sortedDocValuesMessage = Pattern.compile(
			"unexpected docvalues type NONE for field '(.*)' \\(expected one of \\[SORTED, SORTED_SET\\]\\)\\. Use UninvertingReader or index with docvalues\\.");
	private final int segmentNumber;
	private final IndexConfig indexConfig;
	private final AtomicLong counter;
	private final Set<String> fetchSet;
	private final Set<String> fetchSetWithMeta;
	private final Set<String> fetchSetWithDocument;
	private final IndexSegmentInterface indexSegmentInterface;
	private final DocumentStorage documentStorage;
	private IndexWriter indexWriter;
	private DirectoryReader directoryReader;
	private Long lastCommit;
	private Long lastChange;
	private String indexName;
	private QueryResultCache queryResultCache;
	private FacetStateCache facetStateCache;

	private FacetsConfig facetsConfig;
	private int segmentQueryCacheMaxAmount;

	public LumongoSegment(int segmentNumber, IndexSegmentInterface indexSegmentInterface, IndexConfig indexConfig, FacetsConfig facetsConfig,
			DocumentStorage documentStorage) throws Exception {
		setupCaches(indexConfig);

		this.segmentNumber = segmentNumber;
		this.documentStorage = documentStorage;

		this.indexSegmentInterface = indexSegmentInterface;
		this.indexConfig = indexConfig;

		openIndexWriters();

		this.facetsConfig = facetsConfig;

		this.fetchSet = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(LumongoConstants.ID_FIELD, LumongoConstants.TIMESTAMP_FIELD)));

		this.fetchSetWithMeta = Collections
				.unmodifiableSet(new HashSet<>(Arrays.asList(LumongoConstants.ID_FIELD, LumongoConstants.TIMESTAMP_FIELD, LumongoConstants.STORED_META_FIELD)));

		this.fetchSetWithDocument = Collections.unmodifiableSet(new HashSet<>(
				Arrays.asList(LumongoConstants.ID_FIELD, LumongoConstants.TIMESTAMP_FIELD, LumongoConstants.STORED_META_FIELD,
						LumongoConstants.STORED_DOC_FIELD)));

		this.counter = new AtomicLong();
		this.lastCommit = null;
		this.lastChange = null;
		this.indexName = indexConfig.getIndexName();

	}



	private static String getFoldedString(String text) {
		char[] textChar = text.toCharArray();
		char[] output = new char[textChar.length * 4];
		int outputPos = ASCIIFoldingFilter.foldToASCII(textChar, 0, output, 0, textChar.length);
		text = new String(output, 0, outputPos);
		return text;
	}

	private void reopenIndexWritersIfNecessary() throws Exception {
		if (!indexWriter.isOpen()) {
			synchronized (this) {
				if (!indexWriter.isOpen()) {
					this.indexWriter = this.indexSegmentInterface.getIndexWriter(segmentNumber);
					this.directoryReader = DirectoryReader.open(indexWriter, indexConfig.getIndexSettings().getApplyUncommittedDeletes(), false);
				}
			}
		}

	}

	private void openIndexWriters() throws Exception {
		if (this.indexWriter != null) {
			indexWriter.close();
		}
		this.indexWriter = this.indexSegmentInterface.getIndexWriter(segmentNumber);
		this.directoryReader = DirectoryReader.open(indexWriter, indexConfig.getIndexSettings().getApplyUncommittedDeletes(), false);
	}

	private void setupCaches(IndexConfig indexConfig) {
		segmentQueryCacheMaxAmount = indexConfig.getIndexSettings().getSegmentQueryCacheMaxAmount();

		int segmentQueryCacheSize = indexConfig.getIndexSettings().getSegmentQueryCacheSize();
		if ((segmentQueryCacheSize > 0)) {
			this.queryResultCache = new QueryResultCache(segmentQueryCacheSize, 8);
		}
		else {
			this.queryResultCache = null;
		}

		this.facetStateCache = new FacetStateCache(10, 8);

	}

	public void updateIndexSettings(IndexSettings indexSettings, FacetsConfig facetsConfig) throws Exception {

		this.indexConfig.configure(indexSettings);
		this.facetsConfig = facetsConfig;

		setupCaches(indexConfig);
		openIndexWriters();

	}

	public int getSegmentNumber() {
		return segmentNumber;
	}

	public SegmentResponse querySegment(QueryWithFilters queryWithFilters, int amount, FieldDoc after, FacetRequest facetRequest, SortRequest sortRequest,
			QueryCacheKey queryCacheKey, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask, List<Highlight> highlightList) throws Exception {
		try {
			reopenIndexWritersIfNecessary();

			openReaderIfChanges();

			QueryResultCache qrc = queryResultCache;

			boolean useCache = (qrc != null) && ((segmentQueryCacheMaxAmount <= 0) || (segmentQueryCacheMaxAmount >= amount)) && queryCacheKey != null;
			if (useCache) {
				SegmentResponse cacheSegmentResponse = qrc.getCacheSegmentResponse(queryCacheKey);
				if (cacheSegmentResponse != null) {
					return cacheSegmentResponse;
				}
			}

			Query q = queryWithFilters.getQuery();

			if (!queryWithFilters.getFilterQueries().isEmpty()) {
				BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();

				for (Query filterQuery : queryWithFilters.getFilterQueries()) {
					booleanQuery.add(filterQuery, BooleanClause.Occur.FILTER);
				}

				booleanQuery.add(q, BooleanClause.Occur.MUST);

				q = booleanQuery.build();
			}


			List<LumongoHighlighter> highlighterList = new ArrayList<>();

			for (Highlight highlight : highlightList) {
				QueryScorer queryScorer = new QueryScorer(q, highlight.getField());
				Fragmenter fragmenter = new SimpleSpanFragmenter(queryScorer);
				SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter(highlight.getPreTag(), highlight.getPostTag());
				LumongoHighlighter highlighter = new LumongoHighlighter(simpleHTMLFormatter, queryScorer, highlight);
				highlighter.setTextFragmenter(fragmenter);
				highlighterList.add(highlighter);
			}


			IndexSearcher indexSearcher = new IndexSearcher(directoryReader);

			//similarity is only set query time, indexing time all these similarities are the same
			indexSearcher.setSimilarity(new PerFieldSimilarityWrapper() {
				@Override
				public Similarity get(String name) {


					AnalyzerSettings analyzerSettings = indexConfig.getAnalyzerSettingsForIndexField(name);
					AnalyzerSettings.Similarity similarity = AnalyzerSettings.Similarity.BM25;
					if (analyzerSettings != null) {
						similarity = analyzerSettings.getSimilarity();
					}


					AnalyzerSettings.Similarity fieldSimilarityOverride = queryWithFilters.getFieldSimilarityOverride(name);
					if (fieldSimilarityOverride != null) {
						similarity = fieldSimilarityOverride;
					}

					if (AnalyzerSettings.Similarity.TFIDF.equals(similarity)) {
						return new ClassicSimilarity();
					}
					else if (AnalyzerSettings.Similarity.BM25.equals(similarity)) {
						return new BM25Similarity();
					}
					else if (AnalyzerSettings.Similarity.CONSTANT.equals(similarity)) {
						return new ConstantSimilarity();
					}
					else if (AnalyzerSettings.Similarity.TF.equals(similarity)) {
						return new TFSimilarity();
					}
					else {
						throw new RuntimeException("Unknown similarity type <" + similarity + ">");
					}
				}
			});

			int hasMoreAmount = amount + 1;

			TopDocsCollector<?> collector;

			List<SortField> sortFields = new ArrayList<>();
			boolean sorting = (sortRequest != null) && !sortRequest.getFieldSortList().isEmpty();
			if (sorting) {

				for (FieldSort fs : sortRequest.getFieldSortList()) {
					boolean reverse = Direction.DESCENDING.equals(fs.getDirection());

					String sortField = fs.getSortField();
					FieldConfig.FieldType sortFieldType = indexConfig.getFieldTypeForSortField(sortField);

					if (IndexConfigUtil.isNumericOrDateFieldType(sortFieldType)) {
						SortField.Type type;
						if (IndexConfigUtil.isNumericIntFieldType(sortFieldType)) {
							type = SortField.Type.INT;
						}
						else if (IndexConfigUtil.isNumericLongFieldType(sortFieldType)) {
							type = SortField.Type.LONG;
						}
						else if (IndexConfigUtil.isNumericFloatFieldType(sortFieldType)) {
							type = SortField.Type.FLOAT;
						}
						else if (IndexConfigUtil.isNumericDoubleFieldType(sortFieldType)) {
							type = SortField.Type.DOUBLE;
						}
						else if (IndexConfigUtil.isDateFieldType(sortFieldType)) {
							type = SortField.Type.LONG;
						}
						else {
							throw new Exception("Invalid numeric sort type <" + sortFieldType + "> for sort field <" + sortField + ">");
						}
						sortFields.add(new SortedNumericSortField(sortField, type, reverse));
					}
					else {
						sortFields.add(new SortedSetSortField(sortField, reverse));
					}

				}
				Sort sort = new Sort();
				sort.setSort(sortFields.toArray(new SortField[sortFields.size()]));

				collector = TopFieldCollector.create(sort, hasMoreAmount, after, true, true, true);
			}
			else {
				collector = TopScoreDocCollector.create(hasMoreAmount, after);
			}

			SegmentResponse.Builder builder = SegmentResponse.newBuilder();

			if ((facetRequest != null) && !facetRequest.getCountRequestList().isEmpty()) {

				FacetsCollector facetsCollector = new FacetsCollector();
				indexSearcher.search(q, MultiCollector.wrap(collector, facetsCollector));

				for (CountRequest countRequest : facetRequest.getCountRequestList()) {

					String label = countRequest.getFacetField().getLabel();
					String indexFieldName = facetsConfig.getDimConfig(label).indexFieldName;
					if (indexFieldName.equals(FacetsConfig.DEFAULT_INDEX_FIELD_NAME)) {
						throw new Exception(label + " is not defined as a facetable field");
					}


					if (countRequest.hasSegmentFacets()) {
						if (indexConfig.getNumberOfSegments() == 1) {
							log.info("Segment facets is ignored with segments of 1 for facet <" + label + "> on index <" + indexName + ">");
						}
						if (countRequest.getSegmentFacets() < countRequest.getMaxFacets()) {
							throw new IllegalArgumentException("Segment facets must be greater than or equal to max facets");
						}
					}

					int numOfFacets;
					if (indexConfig.getNumberOfSegments() > 1) {
						if (countRequest.getSegmentFacets() != 0) {
							numOfFacets = countRequest.getSegmentFacets();
						}
						else {
							numOfFacets = countRequest.getMaxFacets() * 8;
						}

					}
					else {
						numOfFacets = countRequest.getMaxFacets();
					}

					FacetResult facetResult = null;

					try {

						SortedSetDocValuesReaderState state = facetStateCache.getFacetState(directoryReader, indexFieldName);

						Facets facets = new LumongoSortedSetDocValuesFacetCounts(state, facetsCollector);

						if (countRequest.getSegmentFacets() == 0) {
							numOfFacets = state.getSize();
						}

						facetResult = facets.getTopChildren(numOfFacets, label);
					}
					catch (UncheckedExecutionException e) {
						Throwable cause = e.getCause();
						if (cause.getMessage().contains(" was not indexed with SortedSetDocValues")) {
							//this is when no data has been indexing into a facet
						}
						else {
							throw e;
						}
					}
					handleFacetResult(builder, facetResult, countRequest);
				}

			}
			else {
				indexSearcher.search(q, collector);
			}

			ScoreDoc[] results = collector.topDocs().scoreDocs;

			int totalHits = collector.getTotalHits();

			builder.setTotalHits(totalHits);

			boolean moreAvailable = (results.length == hasMoreAmount);

			int numResults = Math.min(results.length, amount);

			for (int i = 0; i < numResults; i++) {
				ScoredResult.Builder srBuilder = handleDocResult(indexSearcher, sortRequest, sorting, results, i, resultFetchType, fieldsToReturn,
						fieldsToMask, highlighterList);


				builder.addScoredResult(srBuilder.build());
			}

			if (moreAvailable) {
				ScoredResult.Builder srBuilder = handleDocResult(indexSearcher, sortRequest, sorting, results, numResults, resultFetchType, fieldsToReturn,
						fieldsToMask, highlighterList);
				builder.setNext(srBuilder);
			}

			builder.setIndexName(indexName);
			builder.setSegmentNumber(segmentNumber);

			SegmentResponse segmentResponse = builder.build();
			if (useCache) {
				qrc.storeInCache(queryCacheKey, segmentResponse);
			}
			return segmentResponse;
		}
		catch (IllegalStateException e) {
			Matcher m = sortedDocValuesMessage.matcher(e.getMessage());
			if (m.matches()) {
				String field = m.group(1);
				throw new Exception("Field <" + field + "> must have sortAs defined to be sortable");
			}

			throw e;
		}
	}

	private void openReaderIfChanges() throws IOException {
		DirectoryReader newDirectoryReader = DirectoryReader
				.openIfChanged(directoryReader, indexWriter, indexConfig.getIndexSettings().getApplyUncommittedDeletes());
		if (newDirectoryReader != null) {
			directoryReader = newDirectoryReader;
			QueryResultCache qrc = queryResultCache;
			if (qrc != null) {
				qrc.clear();
			}
			FacetStateCache fsc = facetStateCache;
			if (fsc != null) {
				fsc.clear();
			}
		}
	}

	public void handleFacetResult(SegmentResponse.Builder builder, FacetResult fc, CountRequest countRequest) {
		FacetGroup.Builder fg = FacetGroup.newBuilder();
		fg.setCountRequest(countRequest);

		if (fc != null) {

			for (LabelAndValue subResult : fc.labelValues) {
				FacetCount.Builder facetCountBuilder = FacetCount.newBuilder();
				facetCountBuilder.setCount(subResult.value.longValue());
				facetCountBuilder.setFacet(subResult.label);
				fg.addFacetCount(facetCountBuilder);
			}
		}
		builder.addFacetGroup(fg);
	}

	private ScoredResult.Builder handleDocResult(IndexSearcher is, SortRequest sortRequest, boolean sorting, ScoreDoc[] results, int i,
			FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask, List<LumongoHighlighter> highlighterList) throws Exception {
		int docId = results[i].doc;

		Set<String> fieldsToFetch = fetchSet;
		if (indexConfig.getIndexSettings().getStoreDocumentInIndex()) {
			if (FetchType.FULL.equals(resultFetchType)) {
				fieldsToFetch = fetchSetWithDocument;
			}
			else if (FetchType.META.equals(resultFetchType)) {
				fieldsToFetch = fetchSetWithMeta;
			}
		}

		Document d = is.doc(docId, fieldsToFetch);

		IndexableField f = d.getField(LumongoConstants.TIMESTAMP_FIELD);
		long timestamp = f.numericValue().longValue();

		ScoredResult.Builder srBuilder = ScoredResult.newBuilder();
		String uniqueId = d.get(LumongoConstants.ID_FIELD);

		if (!FetchType.NONE.equals(resultFetchType)) {
			if (indexConfig.getIndexSettings().getStoreDocumentInIndex()) {
				ResultDocument.Builder rdBuilder = ResultDocument.newBuilder();
				rdBuilder.setUniqueId(uniqueId);
				rdBuilder.setIndexName(indexName);

				if (FetchType.FULL.equals(resultFetchType) || FetchType.META.equals(resultFetchType)) {
					BytesRef metaRef = d.getBinaryValue(LumongoConstants.STORED_META_FIELD);
					org.bson.Document metaMongoDoc = new org.bson.Document();
					metaMongoDoc.putAll(LumongoUtil.byteArrayToMongoDocument(metaRef.bytes));

					for (String key : metaMongoDoc.keySet()) {
						rdBuilder.addMetadata(Metadata.newBuilder().setKey(key).setValue(((String) metaMongoDoc.get(key))));
					}
				}

				ResultDocument resultDocument = null;

				if (FetchType.FULL.equals(resultFetchType)) {
					BytesRef docRef = d.getBinaryValue(LumongoConstants.STORED_DOC_FIELD);
					rdBuilder.setDocument(ByteString.copyFrom(docRef.bytes));

					if (!fieldsToMask.isEmpty() || !fieldsToReturn.isEmpty()) {
						resultDocument = filterDocument(rdBuilder.build(), fieldsToReturn, fieldsToMask);
					}


					if (!highlighterList.isEmpty()) {
						org.bson.Document doc = ResultHelper.getDocumentFromResultDocument(resultDocument);
						if (doc != null) {
							for (LumongoHighlighter highlighter : highlighterList) {
								String storedFieldName = indexConfig.getStoredFieldName(highlighter.getHighlight().getField());
								LumongoUtil.handleLists(doc.get(storedFieldName), (value) -> {
									String content = value.toString();
									TokenStream tokenStream = analyzer.tokenStream(highlighter.getHighlight().getField(), content);

									try {
										String fragment = highlighter.getBestFragment(tokenStream, content);
										if (fragment != null) {

										}
									}
									catch (Exception e) {
										throw new RuntimeException(e);
									}

								});
								//srBuilder.addHighlihtResult();
							}
						}
					}



				}

				if (resultDocument == null) {
					resultDocument = rdBuilder.build();
				}

				srBuilder.setResultDocument(resultDocument);
			}
			else if (indexConfig.getIndexSettings().getStoreDocumentInMongo()) {

				ResultDocument rd = documentStorage.getSourceDocument(uniqueId, resultFetchType);

				if (rd != null) {
					rd = filterDocument(rd, fieldsToReturn, fieldsToMask);
					srBuilder.setResultDocument(rd);
				}

			}
		}

		srBuilder.setScore(results[i].score);

		srBuilder.setUniqueId(uniqueId);

		srBuilder.setTimestamp(timestamp);

		srBuilder.setDocId(docId);
		srBuilder.setSegment(segmentNumber);
		srBuilder.setIndexName(indexName);
		srBuilder.setResultIndex(i);

		if (sorting) {
			FieldDoc result = (FieldDoc) results[i];

			SortValues.Builder sortValues = SortValues.newBuilder();

			int c = 0;
			for (Object o : result.fields) {
				if (o == null) {
					sortValues.addSortValue(SortValue.newBuilder().setExists(false));
					continue;
				}

				FieldSort fieldSort = sortRequest.getFieldSort(c);
				String sortField = fieldSort.getSortField();

				FieldConfig.FieldType fieldTypeForSortField = indexConfig.getFieldTypeForSortField(sortField);

				SortValue.Builder sortValueBuilder = SortValue.newBuilder().setExists(true);
				if (IndexConfigUtil.isNumericOrDateFieldType(fieldTypeForSortField)) {
					if (IndexConfigUtil.isNumericIntFieldType(fieldTypeForSortField)) {
						sortValueBuilder.setIntegerValue((Integer) o);
					}
					else if (IndexConfigUtil.isNumericLongFieldType(fieldTypeForSortField)) {
						sortValueBuilder.setLongValue((Long) o);
					}
					else if (IndexConfigUtil.isNumericFloatFieldType(fieldTypeForSortField)) {
						sortValueBuilder.setFloatValue((Float) o);
					}
					else if (IndexConfigUtil.isNumericDoubleFieldType(fieldTypeForSortField)) {
						sortValueBuilder.setDoubleValue((Double) o);
					}
					else if (IndexConfigUtil.isDateFieldType(fieldTypeForSortField)) {
						sortValueBuilder.setDateValue((Long) o);
					}
				}
				else {
					BytesRef b = (BytesRef) o;
					sortValueBuilder.setStringValue(b.utf8ToString());
				}
				sortValues.addSortValue(sortValueBuilder);

				c++;
			}
			srBuilder.setSortValues(sortValues);
		}
		return srBuilder;
	}

	public ResultDocument getSourceDocument(String uniqueId, Long timestamp, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask)
			throws Exception {

		ResultDocument rd = null;

		if (indexConfig.getIndexSettings().getStoreDocumentInMongo()) {
			rd = documentStorage.getSourceDocument(uniqueId, resultFetchType);
		}
		else {

			Query query = new TermQuery(new org.apache.lucene.index.Term(LumongoConstants.ID_FIELD, uniqueId));

			QueryWithFilters queryWithFilters = new QueryWithFilters(query);

			SegmentResponse segmentResponse = this.querySegment(queryWithFilters, 1, null, null, null, null, resultFetchType, fieldsToReturn, fieldsToMask, Collections.emptyList());

			List<ScoredResult> scoredResultList = segmentResponse.getScoredResultList();
			if (!scoredResultList.isEmpty()) {
				ScoredResult scoredResult = scoredResultList.iterator().next();
				if (scoredResult.hasResultDocument()) {

					rd = scoredResult.getResultDocument();
				}
			}

		}

		return filterDocument(rd, fieldsToReturn, fieldsToMask);

	}

	private ResultDocument filterDocument(ResultDocument rd, List<String> fieldsToReturn, List<String> fieldsToMask) {
		if (rd != null) {

			if (!fieldsToMask.isEmpty() || !fieldsToReturn.isEmpty()) {
				org.bson.Document resultObj = ResultHelper.getDocumentFromResultDocument(rd);

				ResultDocument.Builder resultDocBuilder = rd.toBuilder();


				if (!fieldsToReturn.isEmpty()) {
					for (String key : new ArrayList<>(resultObj.keySet())) {
						if (!fieldsToReturn.contains(key)) {
							resultObj.remove(key);
						}
					}
				}
				if (!fieldsToMask.isEmpty()) {
					for (String field : fieldsToMask) {
						resultObj.remove(field);
					}
				}

				ByteString document = ByteString.copyFrom(LumongoUtil.mongoDocumentToByteArray(resultObj));
				resultDocBuilder.setDocument(document);

				return resultDocBuilder.build();
			}
			else {
				return rd;
			}
		}

		return null;
	}



	private void possibleCommit() throws IOException {
		lastChange = System.currentTimeMillis();

		long count = counter.incrementAndGet();
		if ((count % indexConfig.getIndexSettings().getSegmentCommitInterval()) == 0) {
			forceCommit();
		}

	}

	public void forceCommit() throws IOException {
		long currentTime = System.currentTimeMillis();

		indexWriter.commit();

		lastCommit = currentTime;

	}

	public void doCommit() throws IOException {

		long currentTime = System.currentTimeMillis();

		Long lastCh = lastChange;
		// if changes since started

		if (lastCh != null) {
			if ((currentTime - lastCh) > (indexConfig.getIndexSettings().getIdleTimeWithoutCommit() * 1000)) {
				if ((lastCommit == null) || (lastCh > lastCommit)) {
					log.info("Flushing segment <" + segmentNumber + "> for index <" + indexName + ">");
					forceCommit();
				}
			}
		}
	}

	public void close() throws IOException {
		forceCommit();

		Directory directory = indexWriter.getDirectory();
		indexWriter.close();
		directory.close();
	}

	public void index(String uniqueId, long timestamp, org.bson.Document mongoDocument, List<Metadata> metadataList) throws Exception {

		reopenIndexWritersIfNecessary();

		Document luceneDocument = new Document();

		for (String storedFieldName : indexConfig.getIndexedStoredFieldNames()) {

			FieldConfig fc = indexConfig.getFieldConfig(storedFieldName);

			if (fc != null) {

				FieldConfig.FieldType fieldType = fc.getFieldType();

				Object o = ResultHelper.getValueFromMongoDocument(mongoDocument, storedFieldName);

				if (o != null) {
					handleFacetsForStoredField(luceneDocument, fc, o);

					handleSortForStoredField(luceneDocument, storedFieldName, fc, o);

					for (IndexAs indexAs : fc.getIndexAsList()) {

						String indexedFieldName = indexAs.getIndexFieldName();
						luceneDocument.add(new StringField(LumongoConstants.FIELDS_LIST_FIELD, indexedFieldName, Store.NO));

						if (FieldConfig.FieldType.NUMERIC_INT.equals(fieldType)) {
							IntFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
						}
						else if (FieldConfig.FieldType.NUMERIC_LONG.equals(fieldType)) {
							LongFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
						}
						else if (FieldConfig.FieldType.NUMERIC_FLOAT.equals(fieldType)) {
							FloatFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
						}
						else if (FieldConfig.FieldType.NUMERIC_DOUBLE.equals(fieldType)) {
							DoubleFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
						}
						else if (FieldConfig.FieldType.DATE.equals(fieldType)) {
							DateFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
						}
						else if (FieldConfig.FieldType.BOOL.equals(fieldType)) {
							BooleanFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
						}
						else if (FieldConfig.FieldType.STRING.equals(fieldType)) {
							StringFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
						}
						else {
							throw new RuntimeException("Unsupported field type <" + fieldType + ">");
						}
					}
				}
			}

		}

		luceneDocument.add(new StringField(LumongoConstants.ID_FIELD, uniqueId, Store.YES));

		luceneDocument.add(new LegacyLongField(LumongoConstants.TIMESTAMP_FIELD, timestamp, Store.YES));

		if (indexConfig.getIndexSettings().getStoreDocumentInIndex()) {
			luceneDocument.add(new StoredField(LumongoConstants.STORED_DOC_FIELD, new BytesRef(LumongoUtil.mongoDocumentToByteArray(mongoDocument))));

			org.bson.Document metadataMongoDoc = new org.bson.Document();

			for (Metadata metadata : metadataList) {
				metadataMongoDoc.put(metadata.getKey(), metadata.getValue());
			}

			luceneDocument.add(new StoredField(LumongoConstants.STORED_META_FIELD, new BytesRef(LumongoUtil.mongoDocumentToByteArray(metadataMongoDoc))));

		}

		Term term = new Term(LumongoConstants.ID_FIELD, uniqueId);

		indexWriter.updateDocument(term, luceneDocument);

		possibleCommit();
	}

	private void handleSortForStoredField(Document d, String storedFieldName, FieldConfig fc, Object o) {

		FieldConfig.FieldType fieldType = fc.getFieldType();
		for (SortAs sortAs : fc.getSortAsList()) {
			String sortFieldName = sortAs.getSortFieldName();

			if (IndexConfigUtil.isNumericOrDateFieldType(fieldType)) {
				LumongoUtil.handleLists(o, obj -> {

					if (FieldConfig.FieldType.DATE.equals(fieldType)) {
						if (obj instanceof Date) {

							Date date = (Date) obj;
							SortedNumericDocValuesField docValue = new SortedNumericDocValuesField(sortFieldName, date.getTime());
							d.add(docValue);
						}
						else {
							throw new RuntimeException(
									"Expecting date for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o.getClass()
											+ ">");
						}
					}
					else {
						if (obj instanceof Number) {

							Number number = (Number) obj;
							SortedNumericDocValuesField docValue = null;
							if (FieldConfig.FieldType.NUMERIC_INT.equals(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, number.intValue());
							}
							else if (FieldConfig.FieldType.NUMERIC_LONG.equals(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, number.longValue());
							}
							else if (FieldConfig.FieldType.NUMERIC_FLOAT.equals(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, NumericUtils.floatToSortableInt(number.floatValue()));
							}
							else if (FieldConfig.FieldType.NUMERIC_DOUBLE.equals(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, NumericUtils.doubleToSortableLong(number.doubleValue()));
							}
							else {
								throw new RuntimeException(
										"Not handled numeric field type <" + fieldType + "> for document field <" + storedFieldName + "> / sort field <"
												+ sortFieldName + ">");
							}

							d.add(docValue);
						}
						else {
							throw new RuntimeException(
									"Expecting number for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o.getClass()
											+ ">");
						}
					}
				});
			}
			else if (FieldConfig.FieldType.BOOL.equals(fieldType)) {
				LumongoUtil.handleLists(o, obj -> {
					if (obj instanceof Boolean) {
						String text = obj.toString();
						SortedSetDocValuesField docValue = new SortedSetDocValuesField(sortFieldName, new BytesRef(text));
						d.add(docValue);
					}
					else {
						throw new RuntimeException(
								"Expecting date for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o.getClass()
										+ ">");
					}
				});
			}
			else if (FieldConfig.FieldType.STRING.equals(fieldType)) {
				LumongoUtil.handleLists(o, obj -> {
					String text = o.toString();

					SortAs.StringHandling stringHandling = sortAs.getStringHandling();
					if (SortAs.StringHandling.STANDARD.equals(stringHandling)) {
						//no op
					}
					else if (SortAs.StringHandling.LOWERCASE.equals(stringHandling)) {
						text = text.toLowerCase();
					}
					else if (SortAs.StringHandling.FOLDING.equals(stringHandling)) {
						text = getFoldedString(text);
					}
					else if (SortAs.StringHandling.LOWERCASE_FOLDING.equals(stringHandling)) {
						text = getFoldedString(text).toLowerCase();
					}
					else {
						throw new RuntimeException(
								"Not handled string handling <" + stringHandling + "> for document field <" + storedFieldName + "> / sort field <"
										+ sortFieldName + ">");
					}

					SortedSetDocValuesField docValue = new SortedSetDocValuesField(sortFieldName, new BytesRef(text));
					d.add(docValue);
				});
			}
			else {
				throw new RuntimeException(
						"Not handled field type <" + fieldType + "> for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">");
			}

		}
	}

	private void handleFacetsForStoredField(Document doc, FieldConfig fc, Object o) throws Exception {
		for (FacetAs fa : fc.getFacetAsList()) {

			String facetName = fa.getFacetName();
			String facetFieldName = facetsConfig.getDimConfig(facetName).indexFieldName;

			if (FieldConfig.FieldType.DATE.equals(fc.getFieldType())) {
				FacetAs.DateHandling dateHandling = fa.getDateHandling();
				LumongoUtil.handleLists(o, obj -> {
					if (obj instanceof Date) {
						LocalDate localDate = ((Date) (obj)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate();

						if (FacetAs.DateHandling.DATE_YYYYMMDD.equals(dateHandling)) {
							String date = FORMATTER_YYYYMMDD.format(localDate);
							addFacet(doc, facetFieldName, date);
						}
						else if (FacetAs.DateHandling.DATE_YYYY_MM_DD.equals(dateHandling)) {
							String date = FORMATTER_YYYY_MM_DD.format(localDate);
							addFacet(doc, facetFieldName, date);
						}
						else {
							throw new RuntimeException("Not handled date handling <" + dateHandling + "> for facet <" + fa.getFacetName() + ">");
						}

					}
					else {
						throw new RuntimeException("Cannot facet date for document field <" + fc.getStoredFieldName() + "> / facet <" + fa.getFacetName()
								+ ">: excepted Date or Collection of Date, found <" + o.getClass().getSimpleName() + ">");
					}
				});
			}
			else {
				LumongoUtil.handleLists(o, obj -> {
					String string = obj.toString();
					addFacet(doc, facetFieldName, string);
				});
			}

		}
	}

	private void addFacet(Document doc, String facetFieldName, String value) {
		if (!value.isEmpty()) {
			doc.add(new SortedSetDocValuesField(facetFieldName, new BytesRef(value)));
			doc.add(new StringField(facetFieldName, new BytesRef(value), Store.NO));
		}
	}

	public void deleteDocument(String uniqueId) throws Exception {
		Term term = new Term(LumongoConstants.ID_FIELD, uniqueId);
		indexWriter.deleteDocuments(term);
		possibleCommit();

	}

	public void optimize() throws IOException {
		lastChange = System.currentTimeMillis();
		indexWriter.forceMerge(1);
		forceCommit();
	}

	public GetFieldNamesResponse getFieldNames() throws IOException {

		openReaderIfChanges();

		GetFieldNamesResponse.Builder builder = GetFieldNamesResponse.newBuilder();

		Set<String> fields = new HashSet<>();

		for (LeafReaderContext subReaderContext : directoryReader.leaves()) {
			FieldInfos fieldInfos = subReaderContext.reader().getFieldInfos();
			for (FieldInfo fi : fieldInfos) {
				String fieldName = fi.name;
				fields.add(fieldName);
			}
		}

		fields.forEach(builder::addFieldName);

		return builder.build();
	}

	public void clear() throws IOException {
		// index has write lock so none needed here
		indexWriter.deleteAll();
		forceCommit();
	}

	public GetTermsResponse getTerms(GetTermsRequest request) throws IOException {
		openReaderIfChanges();

		GetTermsResponse.Builder builder = GetTermsResponse.newBuilder();

		String fieldName = request.getFieldName();

		SortedMap<String, Lumongo.Term.Builder> termsMap = new TreeMap<>();

		if (request.getIncludeTermCount() > 0) {

			Set<String> includeTerms = new TreeSet<>(request.getIncludeTermList());
			List<BytesRef> termBytesList = new ArrayList<>();
			for (String term : includeTerms) {
				BytesRef termBytes = new BytesRef(term);
				termBytesList.add(termBytes);
			}


			for (LeafReaderContext subReaderContext : directoryReader.leaves()) {
				Fields fields = subReaderContext.reader().fields();
				if (fields != null) {

					Terms terms = fields.terms(fieldName);
					if (terms != null) {

						TermsEnum termsEnum = terms.iterator();
						for (BytesRef termBytes : termBytesList) {
							if (termsEnum.seekExact(termBytes)) {
								BytesRef text = termsEnum.term();
								handleTerm(termsMap, termsEnum, text, null, null);
							}

						}
					}
				}
			}
		}
		else {

			BytesRef startTermBytes;
			BytesRef endTermBytes = null;

			if (request.hasStartTerm()) {
				startTermBytes = new BytesRef(request.getStartTerm());
			}
			else {
				startTermBytes = new BytesRef("");
			}

			if (request.hasEndTerm()) {
				endTermBytes = new BytesRef(request.getEndTerm());
			}

			Pattern termFilter = null;
			if (request.hasTermFilter()) {
				termFilter = Pattern.compile(request.getTermFilter());
			}

			Pattern termMatch = null;
			if (request.hasTermMatch()) {
				termMatch = Pattern.compile(request.getTermMatch());
			}

			for (LeafReaderContext subReaderContext : directoryReader.leaves()) {
				Fields fields = subReaderContext.reader().fields();
				if (fields != null) {

					Terms terms = fields.terms(fieldName);
					if (terms != null) {

						TermsEnum termsEnum = terms.iterator();
						SeekStatus seekStatus = termsEnum.seekCeil(startTermBytes);

						if (!seekStatus.equals(SeekStatus.END)) {
							BytesRef text = termsEnum.term();

							if (endTermBytes == null || (text.compareTo(endTermBytes) < 0)) {
								handleTerm(termsMap, termsEnum, text, termFilter, termMatch);

								while ((text = termsEnum.next()) != null) {

									if (endTermBytes == null || (text.compareTo(endTermBytes) < 0)) {
										handleTerm(termsMap, termsEnum, text, termFilter, termMatch);
									}
									else {
										break;
									}
								}
							}
						}

					}
				}

			}
		}

		for (Lumongo.Term.Builder termBuilder : termsMap.values()) {
			builder.addTerm(termBuilder.build());
		}

		return builder.build();

	}

	private void handleTerm(SortedMap<String, Lumongo.Term.Builder> termsMap, TermsEnum termsEnum, BytesRef text, Pattern termFilter, Pattern termMatch)
			throws IOException {

		String textStr = text.utf8ToString();
		if (termFilter != null || termMatch != null) {

			if (termFilter != null) {
				if (termFilter.matcher(textStr).matches()) {
					return;
				}
			}

			if (termMatch != null) {
				if (!termMatch.matcher(textStr).matches()) {
					return;
				}
			}
		}

		if (!termsMap.containsKey(textStr)) {
			termsMap.put(textStr, Lumongo.Term.newBuilder().setValue(textStr).setDocFreq(0).setTermFreq(0));
		}
		Lumongo.Term.Builder builder = termsMap.get(textStr);
		builder.setDocFreq(builder.getDocFreq() + termsEnum.docFreq());
		builder.setTermFreq(builder.getTermFreq() + termsEnum.totalTermFreq());
	}

	public SegmentCountResponse getNumberOfDocs() throws IOException {

		openReaderIfChanges();
		int count = directoryReader.numDocs();
		return SegmentCountResponse.newBuilder().setNumberOfDocs(count).setSegmentNumber(segmentNumber).build();

	}



}
