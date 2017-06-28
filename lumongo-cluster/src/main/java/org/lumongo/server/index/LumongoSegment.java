package org.lumongo.server.index;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import info.debatty.java.lsh.SuperBit;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
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
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.*;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
import org.lumongo.server.config.IndexConfig;
import org.lumongo.server.config.IndexConfigUtil;
import org.lumongo.server.highlighter.LumongoHighlighter;
import org.lumongo.server.index.analysis.AnalysisHandler;
import org.lumongo.server.index.field.BooleanFieldIndexer;
import org.lumongo.server.index.field.DateFieldIndexer;
import org.lumongo.server.index.field.DoubleFieldIndexer;
import org.lumongo.server.index.field.FloatFieldIndexer;
import org.lumongo.server.index.field.IntFieldIndexer;
import org.lumongo.server.index.field.LongFieldIndexer;
import org.lumongo.server.index.field.StringFieldIndexer;
import org.lumongo.server.search.QueryCacheKey;
import org.lumongo.server.search.QueryResultCache;
import org.lumongo.server.search.QueryWithFilters;
import org.lumongo.similarity.ConstantSimilarity;
import org.lumongo.similarity.TFSimilarity;
import org.lumongo.storage.rawfiles.DocumentStorage;
import org.lumongo.util.LumongoUtil;
import org.lumongo.util.ResultHelper;

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

	private FacetsConfig facetsConfig;
	private int segmentQueryCacheMaxAmount;
	private PerFieldAnalyzerWrapper perFieldAnalyzer;

	private DirectoryTaxonomyWriter taxoWriter;
	private DirectoryTaxonomyReader taxoReader;

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

		//TODO: is this a real use case?
		try {
			taxoWriter.getSize();
		}
		catch (AlreadyClosedException e) {
			synchronized (this) {
				this.taxoWriter = this.indexSegmentInterface.getTaxoWriter(segmentNumber);
				this.taxoReader = new DirectoryTaxonomyReader(taxoWriter);
			}
		}

	}

	private void openIndexWriters() throws Exception {
		if (this.indexWriter != null) {
			indexWriter.close();
		}
		if (this.taxoWriter != null) {
			taxoWriter.close();
		}

		this.perFieldAnalyzer = this.indexSegmentInterface.getPerFieldAnalyzer();

		this.indexWriter = this.indexSegmentInterface.getIndexWriter(segmentNumber);
		if (this.directoryReader != null) {
			this.directoryReader.close();
		}
		this.directoryReader = DirectoryReader.open(indexWriter, indexConfig.getIndexSettings().getApplyUncommittedDeletes(), false);

		this.taxoWriter = this.indexSegmentInterface.getTaxoWriter(segmentNumber);
		if (this.taxoReader != null) {
			this.taxoReader.close();
		}
		this.taxoReader = new DirectoryTaxonomyReader(taxoWriter);
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

	}

	public void updateIndexSettings(IndexSettings indexSettings) throws Exception {

		this.indexConfig.configure(indexSettings);

		setupCaches(indexConfig);
		openIndexWriters();

	}

	public int getSegmentNumber() {
		return segmentNumber;
	}

	public SegmentResponse querySegment(QueryWithFilters queryWithFilters, int amount, FieldDoc after, FacetRequest facetRequest, SortRequest sortRequest,
			QueryCacheKey queryCacheKey, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask,
			List<HighlightRequest> highlightList, List<AnalysisRequest> analysisRequestList, boolean debug) throws Exception {
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

			if (!queryWithFilters.getFilterQueries().isEmpty() || !queryWithFilters.getScoredFilterQueries().isEmpty()) {
				BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();

				for (Query filterQuery : queryWithFilters.getFilterQueries()) {
					booleanQuery.add(filterQuery, BooleanClause.Occur.FILTER);
				}

				for (Query scoredFilterQuery : queryWithFilters.getScoredFilterQueries()) {
					booleanQuery.add(scoredFilterQuery, BooleanClause.Occur.MUST);
				}

				booleanQuery.add(q, BooleanClause.Occur.MUST);

				q = booleanQuery.build();
			}

			IndexSearcher indexSearcher = new IndexSearcher(directoryReader);

			//similarity is only set query time, indexing time all these similarities are the same
			indexSearcher.setSimilarity(getSimilarity(queryWithFilters));

			if (debug) {
				log.info("Lucene Query for index <" + indexName + "> segment <" + segmentNumber + ">: " + q);
				log.info("Rewritten Query for index <" + indexName + "> segment <" + segmentNumber + ">: " + indexSearcher.rewrite(q));
			}

			int hasMoreAmount = amount + 1;

			TopDocsCollector<?> collector;

			boolean sorting = (sortRequest != null) && !sortRequest.getFieldSortList().isEmpty();
			if (sorting) {

				collector = getSortingCollector(sortRequest, hasMoreAmount, after);
			}
			else {
				collector = TopScoreDocCollector.create(hasMoreAmount, after);
			}

			SegmentResponse.Builder segmentReponseBuilder = SegmentResponse.newBuilder();

			if ((facetRequest != null) && !facetRequest.getCountRequestList().isEmpty()) {

				searchWithFacets(facetRequest, q, indexSearcher, collector, segmentReponseBuilder);

			}
			else {
				indexSearcher.search(q, collector);
			}

			ScoreDoc[] results = collector.topDocs().scoreDocs;

			int totalHits = collector.getTotalHits();

			segmentReponseBuilder.setTotalHits(totalHits);

			boolean moreAvailable = (results.length == hasMoreAmount);

			int numResults = Math.min(results.length, amount);

			List<LumongoHighlighter> highlighterList = getHighlighterList(highlightList, q);

			List<AnalysisHandler> analysisHandlerList = getAnalysisHandlerList(analysisRequestList);

			for (int i = 0; i < numResults; i++) {
				ScoredResult.Builder srBuilder = handleDocResult(indexSearcher, sortRequest, sorting, results, i, resultFetchType, fieldsToReturn, fieldsToMask,
						highlighterList, analysisHandlerList);

				segmentReponseBuilder.addScoredResult(srBuilder.build());
			}

			if (moreAvailable) {
				ScoredResult.Builder srBuilder = handleDocResult(indexSearcher, sortRequest, sorting, results, numResults, FetchType.NONE,
						Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
				segmentReponseBuilder.setNext(srBuilder);
			}

			segmentReponseBuilder.setIndexName(indexName);
			segmentReponseBuilder.setSegmentNumber(segmentNumber);

			if (!analysisHandlerList.isEmpty()) {
				for (AnalysisHandler analysisHandler : analysisHandlerList) {
					AnalysisResult segmentAnalysisResult = analysisHandler.getSegmentResult();
					if (segmentAnalysisResult != null) {
						segmentReponseBuilder.addAnalysisResult(segmentAnalysisResult);
					}
				}
			}

			SegmentResponse segmentResponse = segmentReponseBuilder.build();
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

	private List<AnalysisHandler> getAnalysisHandlerList(List<AnalysisRequest> analysisRequests) throws Exception {
		if (analysisRequests.isEmpty()) {
			return Collections.emptyList();
		}

		List<AnalysisHandler> analysisHandlerList = new ArrayList<>();
		for (AnalysisRequest analysisRequest : analysisRequests) {

			Analyzer analyzer = perFieldAnalyzer;

			if (analysisRequest.hasAnalyzerOverride()) {
				String analyzerName = analysisRequest.getAnalyzerOverride();

				AnalyzerSettings analyzerSettings = indexConfig.getAnalyzerSettingsByName(analyzerName);
				if (analyzerSettings != null) {
					analyzer = LumongoAnalyzerFactory.getPerFieldAnalyzer(analyzerSettings);
				}
				else {
					throw new RuntimeException("Invalid analyzer name <" + analyzerName + ">");
				}
			}
			System.out.println(analysisRequest.getAnalyzerOverride());
			AnalysisHandler analysisHandler = new AnalysisHandler(directoryReader, analyzer, indexConfig, analysisRequest);
			analysisHandlerList.add(analysisHandler);
		}
		return analysisHandlerList;

	}

	private List<LumongoHighlighter> getHighlighterList(List<HighlightRequest> highlightRequests, Query q) {

		if (highlightRequests.isEmpty()) {
			return Collections.emptyList();
		}

		List<LumongoHighlighter> highlighterList = new ArrayList<>();

		for (HighlightRequest highlight : highlightRequests) {
			QueryScorer queryScorer = new QueryScorer(q, highlight.getField());
			queryScorer.setExpandMultiTermQuery(true);
			Fragmenter fragmenter = new SimpleSpanFragmenter(queryScorer, highlight.getFragmentLength());
			SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter(highlight.getPreTag(), highlight.getPostTag());
			LumongoHighlighter highlighter = new LumongoHighlighter(simpleHTMLFormatter, queryScorer, highlight);
			highlighter.setTextFragmenter(fragmenter);
			highlighterList.add(highlighter);
		}
		return highlighterList;
	}

	private PerFieldSimilarityWrapper getSimilarity(final QueryWithFilters queryWithFilters) {
		return new PerFieldSimilarityWrapper() {
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
		};
	}

	private void searchWithFacets(FacetRequest facetRequest, Query q, IndexSearcher indexSearcher, TopDocsCollector<?> collector,
			SegmentResponse.Builder segmentReponseBuilder) throws Exception {
		FacetsCollector facetsCollector = new FacetsCollector();
		indexSearcher.search(q, MultiCollector.wrap(collector, facetsCollector));

		Facets facets = new FastTaxonomyFacetCounts(taxoReader, facetsConfig, facetsCollector);

		for (CountRequest countRequest : facetRequest.getCountRequestList()) {

			String label = countRequest.getFacetField().getLabel();

			if (!indexConfig.existingFacet(label)) {
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

				if (indexConfig.getNumberOfSegments() > 1) {
					if (countRequest.hasSegmentFacets() && countRequest.getSegmentFacets() == 0) {
						//TODO: this not ideal
						numOfFacets = taxoReader.getSize();
					}
				}

				facetResult = facets.getTopChildren(numOfFacets, label);
			}
			catch (UncheckedExecutionException e) {
				Throwable cause = e.getCause();
				if (cause.getMessage().contains(" was not indexed with SortedSetDocValues")) {
					//this is when no data has been indexing into a facet or facet does not exist
				}
				else {
					throw e;
				}
			}
			FacetGroup.Builder fg = FacetGroup.newBuilder();
			fg.setCountRequest(countRequest);

			if (facetResult != null) {

				for (LabelAndValue subResult : facetResult.labelValues) {
					FacetCount.Builder facetCountBuilder = FacetCount.newBuilder();
					facetCountBuilder.setCount(subResult.value.longValue());
					facetCountBuilder.setFacet(subResult.label);
					fg.addFacetCount(facetCountBuilder);
				}
			}
			segmentReponseBuilder.addFacetGroup(fg);
		}
	}

	private TopDocsCollector<?> getSortingCollector(SortRequest sortRequest, int hasMoreAmount, FieldDoc after) throws Exception {
		List<SortField> sortFields = new ArrayList<>();
		TopDocsCollector<?> collector;
		for (FieldSort fs : sortRequest.getFieldSortList()) {
			boolean reverse = Direction.DESCENDING.equals(fs.getDirection());

			String sortField = fs.getSortField();
			FieldConfig.FieldType sortFieldType = indexConfig.getFieldTypeForSortField(sortField);

			if (IndexConfigUtil.isNumericOrDateFieldType(sortFieldType)) {

				SortedNumericSelector.Type sortedNumericSelector = SortedNumericSelector.Type.MIN;
				if (reverse) {
					sortedNumericSelector = SortedNumericSelector.Type.MAX;
				}

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
				sortFields.add(new SortedNumericSortField(sortField, type, reverse, sortedNumericSelector));
			}
			else {

				SortedSetSelector.Type sortedSetSelector = SortedSetSelector.Type.MIN;
				if (reverse) {
					sortedSetSelector = SortedSetSelector.Type.MAX;
				}

				sortFields.add(new SortedSetSortField(sortField, reverse, sortedSetSelector));
			}

		}
		Sort sort = new Sort();
		sort.setSort(sortFields.toArray(new SortField[sortFields.size()]));

		collector = TopFieldCollector.create(sort, hasMoreAmount, after, true, true, true);
		return collector;
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

		}

		DirectoryTaxonomyReader newone = TaxonomyReader.openIfChanged(taxoReader);
		if (newone != null) {
			taxoReader = newone;
		}
	}

	private ScoredResult.Builder handleDocResult(IndexSearcher is, SortRequest sortRequest, boolean sorting, ScoreDoc[] results, int i,
			FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask, List<LumongoHighlighter> highlighterList,
			List<AnalysisHandler> analysisHandlerList) throws Exception {
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

		if (!highlighterList.isEmpty() && !FetchType.FULL.equals(resultFetchType)) {
			throw new Exception("Highlighting requires a full fetch of the document");
		}

		if (!analysisHandlerList.isEmpty() && !FetchType.FULL.equals(resultFetchType)) {
			throw new Exception("Analysis requires a full fetch of the document");
		}

		if (!FetchType.NONE.equals(resultFetchType)) {
			handleStoredDoc(srBuilder, uniqueId, d, resultFetchType, fieldsToReturn, fieldsToMask, highlighterList, analysisHandlerList);
		}

		srBuilder.setScore(results[i].score);

		srBuilder.setUniqueId(uniqueId);

		srBuilder.setTimestamp(timestamp);

		srBuilder.setDocId(docId);
		srBuilder.setSegment(segmentNumber);
		srBuilder.setIndexName(indexName);
		srBuilder.setResultIndex(i);

		if (sorting) {
			handleSortValues(sortRequest, results[i], srBuilder);
		}
		return srBuilder;
	}

	private void handleStoredDoc(ScoredResult.Builder srBuilder, String uniqueId, Document d, FetchType resultFetchType, List<String> fieldsToReturn,
			List<String> fieldsToMask, List<LumongoHighlighter> highlighterList, List<AnalysisHandler> analysisHandlerList) throws Exception {

		ResultDocument.Builder rdBuilder = ResultDocument.newBuilder();
		rdBuilder.setUniqueId(uniqueId);
		rdBuilder.setIndexName(indexName);

		ResultDocument resultDocument = null;

		if (indexConfig.getIndexSettings().getStoreDocumentInIndex()) {

			if (FetchType.FULL.equals(resultFetchType) || FetchType.META.equals(resultFetchType)) {
				BytesRef metaRef = d.getBinaryValue(LumongoConstants.STORED_META_FIELD);
				org.bson.Document metaMongoDoc = new org.bson.Document();
				metaMongoDoc.putAll(LumongoUtil.byteArrayToMongoDocument(metaRef.bytes));

				for (String key : metaMongoDoc.keySet()) {
					rdBuilder.addMetadata(Metadata.newBuilder().setKey(key).setValue(((String) metaMongoDoc.get(key))));
				}
			}

			if (FetchType.FULL.equals(resultFetchType)) {
				BytesRef docRef = d.getBinaryValue(LumongoConstants.STORED_DOC_FIELD);
				if (docRef != null) {
					rdBuilder.setDocument(ByteString.copyFrom(docRef.bytes));
				}
			}

		}
		else if (indexConfig.getIndexSettings().getStoreDocumentInMongo()) {
			resultDocument = documentStorage.getSourceDocument(uniqueId, resultFetchType);
		}

		if (resultDocument == null) {
			resultDocument = rdBuilder.build();
		}

		if (!highlighterList.isEmpty() || !analysisHandlerList.isEmpty() || !fieldsToMask.isEmpty() || !fieldsToReturn.isEmpty()) {
			org.bson.Document mongoDoc = ResultHelper.getDocumentFromResultDocument(resultDocument);
			if (mongoDoc != null) {
				if (!highlighterList.isEmpty()) {
					handleHighlight(highlighterList, srBuilder, mongoDoc);
				}
				if (!analysisHandlerList.isEmpty()) {
					AnalysisHandler.handleDocument(mongoDoc, analysisHandlerList, srBuilder);
				}

				resultDocument = filterDocument(resultDocument, fieldsToReturn, fieldsToMask, mongoDoc);
			}
		}

		srBuilder.setResultDocument(resultDocument);
	}

	private void handleSortValues(SortRequest sortRequest, ScoreDoc scoreDoc, ScoredResult.Builder srBuilder) {
		FieldDoc result = (FieldDoc) scoreDoc;

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

	private void handleHighlight(List<LumongoHighlighter> highlighterList, ScoredResult.Builder srBuilder, org.bson.Document doc) {

		for (LumongoHighlighter highlighter : highlighterList) {
			HighlightRequest highlightRequest = highlighter.getHighlight();
			String indexField = highlightRequest.getField();
			String storedFieldName = indexConfig.getStoredFieldName(indexField);

			if (storedFieldName != null) {
				HighlightResult.Builder highLightResult = HighlightResult.newBuilder();
				highLightResult.setField(storedFieldName);

				Object storeFieldValues = ResultHelper.getValueFromMongoDocument(doc, storedFieldName);

				LumongoUtil.handleLists(storeFieldValues, (value) -> {
					String content = value.toString();
					TokenStream tokenStream = perFieldAnalyzer.tokenStream(indexField, content);

					try {
						TextFragment[] bestTextFragments = highlighter
								.getBestTextFragments(tokenStream, content, false, highlightRequest.getNumberOfFragments());
						for (TextFragment bestTextFragment : bestTextFragments) {
							if (bestTextFragment != null && bestTextFragment.getScore() > 0) {
								highLightResult.addFragments(bestTextFragment.toString());
							}
						}
					}
					catch (Exception e) {
						throw new RuntimeException(e);
					}

				});

				srBuilder.addHighlightResult(highLightResult);
			}

		}

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

			SegmentResponse segmentResponse = this
					.querySegment(queryWithFilters, 1, null, null, null, null, resultFetchType, fieldsToReturn, fieldsToMask, Collections.emptyList(),
							Collections.emptyList(), false);

			List<ScoredResult> scoredResultList = segmentResponse.getScoredResultList();
			if (!scoredResultList.isEmpty()) {
				ScoredResult scoredResult = scoredResultList.iterator().next();
				if (scoredResult.hasResultDocument()) {
					rd = scoredResult.getResultDocument();
				}
			}

		}

		if (rd != null) {
			if (!fieldsToMask.isEmpty() || !fieldsToReturn.isEmpty()) {
				org.bson.Document mongoDocument = ResultHelper.getDocumentFromResultDocument(rd);
				if (mongoDocument != null) {
					rd = filterDocument(rd, fieldsToReturn, fieldsToMask, mongoDocument);
				}
			}
			return rd;
		}

		ResultDocument.Builder rdBuilder = ResultDocument.newBuilder();
		rdBuilder.setUniqueId(uniqueId);
		rdBuilder.setIndexName(indexName);
		return rdBuilder.build();

	}

	private ResultDocument filterDocument(ResultDocument rd, List<String> fieldsToReturn, List<String> fieldsToMask, org.bson.Document mongoDocument) {

		ResultDocument.Builder resultDocBuilder = rd.toBuilder();

		if (!fieldsToReturn.isEmpty()) {
			for (String key : new ArrayList<>(mongoDocument.keySet())) {
				if (!fieldsToReturn.contains(key)) {
					mongoDocument.remove(key);
				}
			}
		}
		if (!fieldsToMask.isEmpty()) {
			for (String field : fieldsToMask) {
				mongoDocument.remove(field);
			}
		}

		ByteString document = ByteString.copyFrom(LumongoUtil.mongoDocumentToByteArray(mongoDocument));
		resultDocBuilder.setDocument(document);

		return resultDocBuilder.build();

	}

	private void possibleCommit() throws IOException {
		lastChange = System.currentTimeMillis();

		long count = counter.incrementAndGet();
		if ((count % indexConfig.getIndexSettings().getSegmentCommitInterval()) == 0) {
			forceCommit();
		}

	}

	public void forceCommit() throws IOException {
		log.info("Committing segment <" + segmentNumber + "> for index <" + indexName + ">");
		long currentTime = System.currentTimeMillis();
		indexWriter.commit();
		taxoWriter.commit();

		lastCommit = currentTime;

	}

	public void doCommit() throws IOException {

		long currentTime = System.currentTimeMillis();

		Long lastCh = lastChange;
		// if changes since started

		if (lastCh != null) {
			if ((currentTime - lastCh) > (indexConfig.getIndexSettings().getIdleTimeWithoutCommit() * 1000)) {
				if ((lastCommit == null) || (lastCh > lastCommit)) {
					forceCommit();
				}
			}
		}
	}

	public void close(boolean terminate) throws IOException {
		if (!terminate) {
			forceCommit();
		}

		Directory directory = indexWriter.getDirectory();
		indexWriter.close();
		directory.close();

		directory = taxoWriter.getDirectory();
		taxoWriter.close();
		directory.close();
	}

	public void index(String uniqueId, long timestamp, org.bson.Document mongoDocument, List<Metadata> metadataList) throws Exception {

		reopenIndexWritersIfNecessary();

		Document luceneDocument = new Document();

		addStoredFieldsForDocument(mongoDocument, luceneDocument);

		luceneDocument.add(new StringField(LumongoConstants.ID_FIELD, uniqueId, Store.YES));

		luceneDocument.add(new LongPoint(LumongoConstants.TIMESTAMP_FIELD, timestamp));
		luceneDocument.add(new StoredField(LumongoConstants.TIMESTAMP_FIELD, timestamp));

		if (indexConfig.getIndexSettings().getStoreDocumentInIndex()) {
			luceneDocument.add(new StoredField(LumongoConstants.STORED_DOC_FIELD, new BytesRef(LumongoUtil.mongoDocumentToByteArray(mongoDocument))));

			org.bson.Document metadataMongoDoc = new org.bson.Document();

			for (Metadata metadata : metadataList) {
				metadataMongoDoc.put(metadata.getKey(), metadata.getValue());
			}

			luceneDocument.add(new StoredField(LumongoConstants.STORED_META_FIELD, new BytesRef(LumongoUtil.mongoDocumentToByteArray(metadataMongoDoc))));

		}

		luceneDocument = facetsConfig.build(taxoWriter, luceneDocument);

		Term term = new Term(LumongoConstants.ID_FIELD, uniqueId);

		indexWriter.updateDocument(term, luceneDocument);

		possibleCommit();
	}

	private void addStoredFieldsForDocument(org.bson.Document mongoDocument, Document luceneDocument) throws Exception {
		for (String storedFieldName : indexConfig.getIndexedStoredFieldNames()) {

			FieldConfig fc = indexConfig.getFieldConfig(storedFieldName);

			if (fc != null) {

				FieldConfig.FieldType fieldType = fc.getFieldType();

				Object o = ResultHelper.getValueFromMongoDocument(mongoDocument, storedFieldName);

				if (o != null) {
					handleFacetsForStoredField(luceneDocument, fc, o);

					handleSortForStoredField(luceneDocument, storedFieldName, fc, o);

					handleIndexingForStoredField(luceneDocument, storedFieldName, fc, fieldType, o);

					handleProjectForStoredField(luceneDocument, fc, o);
				}
			}

		}
	}

	private void handleProjectForStoredField(Document luceneDocument, FieldConfig fc, Object o) throws Exception {
		for (ProjectAs projectAs : fc.getProjectAsList()) {
			if (projectAs.hasSuperbit()) {
				if (o instanceof List) {
					List<Number> values = (List<Number>) o;

					double vec[] = new double[values.size()];
					int i = 0;
					for (Number value : values) {
						vec[i++] = value.doubleValue();
					}

					SuperBit superBitForField = indexConfig.getSuperBitForField(projectAs.getField());
					boolean[] signature = superBitForField.signature(vec);

					int j = 0;
					for (boolean s : signature) {
						StringFieldIndexer.INSTANCE.index(luceneDocument, projectAs.getField(), s ? "1" : "0",
								LumongoConstants.SUPERBIT_PREFIX + "." + projectAs.getField() + "." + j);
						j++;
					}

				}
				else {
					throw new Exception("Expecting a list for superbit field <" + projectAs.getField() + ">");
				}
			}
		}
	}

	private void handleIndexingForStoredField(Document luceneDocument, String storedFieldName, FieldConfig fc, FieldConfig.FieldType fieldType, Object o)
			throws Exception {
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
								"Expecting boolean for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o.getClass()
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

			if (FieldConfig.FieldType.DATE.equals(fc.getFieldType())) {
				FacetAs.DateHandling dateHandling = fa.getDateHandling();
				LumongoUtil.handleLists(o, obj -> {
					if (obj instanceof Date) {
						LocalDate localDate = ((Date) (obj)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate();

						if (FacetAs.DateHandling.DATE_YYYYMMDD.equals(dateHandling)) {
							String date = FORMATTER_YYYYMMDD.format(localDate);
							addFacet(doc, facetName, date);
						}
						else if (FacetAs.DateHandling.DATE_YYYY_MM_DD.equals(dateHandling)) {
							String date = FORMATTER_YYYY_MM_DD.format(localDate);
							addFacet(doc, facetName, date);
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
					addFacet(doc, facetName, string);
				});
			}

		}
	}

	private void addFacet(Document doc, String facetName, String value) {
		if (!value.isEmpty()) {
			doc.add(new FacetField(facetName, value));
			doc.add(new StringField(FacetsConfig.DEFAULT_INDEX_FIELD_NAME + "." + facetName, new BytesRef(value), Store.NO));
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

			AttributeSource atts = null;
			MaxNonCompetitiveBoostAttribute maxBoostAtt = null;
			if (request.hasFuzzyTerm()) {
				atts = new AttributeSource();
				maxBoostAtt = atts.addAttribute(MaxNonCompetitiveBoostAttribute.class);
			}

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

						if (request.hasFuzzyTerm()) {
							FuzzyTerm fuzzyTerm = request.getFuzzyTerm();
							FuzzyTermsEnum termsEnum = new FuzzyTermsEnum(terms, atts, new Term(fieldName, fuzzyTerm.getTerm()), fuzzyTerm.getEditDistance(),
									fuzzyTerm.getPrefixLength(), fuzzyTerm.getTranspositions());
							BytesRef text = termsEnum.term();

							handleTerm(termsMap, termsEnum, text, termFilter, termMatch);

							while ((text = termsEnum.next()) != null) {
								handleTerm(termsMap, termsEnum, text, termFilter, termMatch);
							}

						}
						else {
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
		BoostAttribute boostAttribute = termsEnum.attributes().getAttribute(BoostAttribute.class);
		if (boostAttribute != null) {
			builder.setScore(boostAttribute.getBoost());
		}

	}

	public SegmentCountResponse getNumberOfDocs() throws IOException {

		openReaderIfChanges();
		int count = directoryReader.numDocs();
		return SegmentCountResponse.newBuilder().setNumberOfDocs(count).setSegmentNumber(segmentNumber).build();

	}

}
