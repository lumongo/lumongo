package org.lumongo.server.indexing;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.directory.LumongoDirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.LumongoDirectoryTaxonomyWriter;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LumongoIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.BytesRef;
import org.bson.BSONObject;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.CountRequest;
import org.lumongo.cluster.message.Lumongo.FacetAs;
import org.lumongo.cluster.message.Lumongo.FacetAs.LMFacetType;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FacetGroup;
import org.lumongo.cluster.message.Lumongo.FacetRequest;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
import org.lumongo.cluster.message.Lumongo.FieldSort;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;
import org.lumongo.cluster.message.Lumongo.GetTermsRequest;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;
import org.lumongo.cluster.message.Lumongo.IndexAs;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.cluster.message.Lumongo.SegmentCountResponse;
import org.lumongo.cluster.message.Lumongo.SegmentResponse;
import org.lumongo.cluster.message.Lumongo.SortRequest;
import org.lumongo.server.config.IndexConfig;
import org.lumongo.server.indexing.field.DateFieldIndexer;
import org.lumongo.server.indexing.field.DoubleFieldIndexer;
import org.lumongo.server.indexing.field.FloatFieldIndexer;
import org.lumongo.server.indexing.field.IntFieldIndexer;
import org.lumongo.server.indexing.field.LongFieldIndexer;
import org.lumongo.server.indexing.field.StringFieldIndexer;
import org.lumongo.server.searching.QueryWithFilters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class LumongoSegment {

	private final static DateTimeFormatter FORMATTER_YYYY_MM_DD = DateTimeFormat.forPattern("yyyyMMdd").withZoneUTC();

	private final static Logger log = Logger.getLogger(LumongoSegment.class);

	private final int segmentNumber;

	private final LumongoIndexWriter indexWriter;
	private LumongoDirectoryTaxonomyWriter taxonomyWriter;
	private LumongoDirectoryTaxonomyReader taxonomyReader;

	private final IndexConfig indexConfig;
	private final FacetsConfig facetsConfig;

	private final String uniqueIdField;

	private final AtomicLong counter;

	private Long lastCommit;
	private Long lastChange;
	private String indexName;

	private final Set<String> fetchSet;

	private QueryResultCache queryResultCache;
	private QueryResultCache queryResultCacheRealtime;

	private boolean queryCacheEnabled;

	private int segmentQueryCacheMaxAmount;

	public LumongoSegment(int segmentNumber, LumongoIndexWriter indexWriter, LumongoDirectoryTaxonomyWriter taxonomyWriter, IndexConfig indexConfig)
					throws IOException {

		setupQueryCache(indexConfig);

		this.segmentNumber = segmentNumber;

		this.indexWriter = indexWriter;

		this.taxonomyWriter = taxonomyWriter;
		if (this.taxonomyWriter != null) {
			this.taxonomyReader = new LumongoDirectoryTaxonomyReader(taxonomyWriter);
		}

		this.indexConfig = indexConfig;
		this.facetsConfig = getFacetsConfig();

		this.uniqueIdField = indexConfig.getUniqueIdField();

		this.fetchSet = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(uniqueIdField, LumongoConstants.TIMESTAMP_FIELD)));

		this.counter = new AtomicLong();
		this.lastCommit = null;
		this.lastChange = null;
		this.indexName = indexConfig.getIndexName();

	}

	protected FacetsConfig getFacetsConfig() {
		//only need to be done once but no harm
		FacetsConfig.DEFAULT_DIM_CONFIG.hierarchical = true;
		FacetsConfig.DEFAULT_DIM_CONFIG.multiValued = true;
		return new FacetsConfig() {
			@Override
			public synchronized DimConfig getDimConfig(String dimName) {
				DimConfig dc = new DimConfig();
				dc.multiValued = true;
				dc.hierarchical = true;
				return dc;
			}
		};
	}
	
	public static Object getValueFromDocument(BSONObject document, String storedFieldName) {

		Object o;
		if (storedFieldName.contains(".")) {
			o = document;
			String[] fields = storedFieldName.split("\\.");
			for (String field : fields) {
				if (o instanceof List) {
					List list = (List) o;
					List<Object> values = new ArrayList<>();
					for (Object item : list) {
						if (item instanceof BSONObject) {
							BSONObject dbObj = (BSONObject) item;
							Object object = dbObj.get(field);
							if (object != null) {
								values.add(object);
							}
						}
					}
					o = values;
				}
				else if (o instanceof BSONObject) {
					BSONObject dbObj = (BSONObject) o;
					if (dbObj != null) {
						o = dbObj.get(field);
					}
					else {
						o = null;
						break;
					}
				}
				else {
					o = null;
					break;
				}
			}
		}
		else {
			o = document.get(storedFieldName);
		}

		return o;
	}
	
	private void setupQueryCache(IndexConfig indexConfig) {
		queryCacheEnabled = (indexConfig.getSegmentQueryCacheSize() > 0);
		segmentQueryCacheMaxAmount = indexConfig.getSegmentQueryCacheMaxAmount();

		if (queryCacheEnabled) {
			this.queryResultCache = new QueryResultCache(indexConfig.getSegmentQueryCacheSize(), 8);
			this.queryResultCacheRealtime = new QueryResultCache(indexConfig.getSegmentQueryCacheSize(), 8);
		}
	}

	public void updateIndexSettings(IndexSettings indexSettings, LumongoIndexWriter indexWriter, LumongoDirectoryTaxonomyWriter taxonomyWriter) {

		this.indexConfig.configure(indexSettings);
		setupQueryCache(indexConfig);
	}

	public int getSegmentNumber() {
		return segmentNumber;
	}

	public SegmentResponse querySegment(QueryWithFilters queryWithFilters, int amount, FieldDoc after, FacetRequest facetRequest, SortRequest sortRequest,
					boolean realTime, QueryCacheKey queryCacheKey) throws Exception {

		IndexReader ir = null;

		try {

			QueryResultCache qrc = realTime ? queryResultCacheRealtime : queryResultCache;

			boolean useCache = queryCacheEnabled && ((segmentQueryCacheMaxAmount <= 0) || (segmentQueryCacheMaxAmount >= amount));
			if (useCache) {
				SegmentResponse cacheSegmentResponse = qrc.getCacheSegmentResponse(queryCacheKey);
				if (cacheSegmentResponse != null) {
					return cacheSegmentResponse;
				}

			}

			Query q = queryWithFilters.getQuery();

			if (!queryWithFilters.getFilterQueries().isEmpty()) {
				BooleanFilter bf = new BooleanFilter();

				for (Query filterQuery : queryWithFilters.getFilterQueries()) {
					Filter f = new QueryWrapperFilter(filterQuery);
					bf.add(f, BooleanClause.Occur.MUST);
				}

				q = new FilteredQuery(q, bf);
			}

			ir = indexWriter.getReader(indexConfig.getApplyUncommitedDeletes(), realTime);

			IndexSearcher is = new IndexSearcher(ir);

			int hasMoreAmount = amount + 1;

			TopDocsCollector<?> collector;

			List<SortField> sortFields = new ArrayList<SortField>();
			boolean sorting = (sortRequest != null) && !sortRequest.getFieldSortList().isEmpty();
			if (sorting) {

				for (FieldSort fs : sortRequest.getFieldSortList()) {
					boolean reverse = Direction.DESCENDING.equals(fs.getDirection());

					String sortField = fs.getSortField();

					SortField.Type type = SortField.Type.STRING;
					if (indexConfig.isNumericOrDateField(sortField)) {
						if (indexConfig.isNumericIntField(sortField)) {
							type = SortField.Type.INT;
						}
						else if (indexConfig.isNumericLongField(sortField)) {
							type = SortField.Type.LONG;
						}
						else if (indexConfig.isNumericFloatField(sortField)) {
							type = SortField.Type.FLOAT;
						}
						else if (indexConfig.isNumericDoubleField(sortField)) {
							type = SortField.Type.DOUBLE;
						}
						else if (indexConfig.isDateField(sortField)) {
							type = SortField.Type.LONG;
						}
					}

					sortFields.add(new SortField(sortField, type, reverse));
				}
				Sort sort = new Sort();
				sort.setSort(sortFields.toArray(new SortField[0]));
				boolean fillFields = true;
				boolean trackDocScores = true;
				boolean trackMaxScore = true;
				collector = TopFieldCollector.create(sort, hasMoreAmount, after, fillFields, trackDocScores, trackMaxScore);
			}
			else {
				collector = TopScoreDocCollector.create(hasMoreAmount, after);
			}

			SegmentResponse.Builder builder = SegmentResponse.newBuilder();

			if ((facetRequest != null) && !facetRequest.getCountRequestList().isEmpty()) {

				taxonomyReader = taxonomyReader.doOpenIfChanged(realTime);

				int maxFacets = Integer.MAX_VALUE; // have to fetch all facets to merge between segments correctly

				if (facetRequest.getDrillSideways()) {
					DrillSideways ds = new DrillSideways(is, facetsConfig, taxonomyReader);
					DrillSidewaysResult ddsr = ds.search((DrillDownQuery) q, collector);
					for (CountRequest countRequest : facetRequest.getCountRequestList()) {
						FacetResult facetResult = ddsr.facets.getTopChildren(maxFacets, countRequest.getFacetField().getLabel(),
										countRequest.getFacetField().getPathList().toArray(new String[0]));

						handleFacetResult(builder, facetResult, countRequest);

					}

				}
				else {

					FacetsCollector fc = new FacetsCollector();
					is.search(q, MultiCollector.wrap(collector, fc));
					Facets facets = new FastTaxonomyFacetCounts(taxonomyReader, facetsConfig, fc);
					for (CountRequest countRequest : facetRequest.getCountRequestList()) {
						FacetResult facetResult = facets.getTopChildren(maxFacets, countRequest.getFacetField().getLabel(),
										countRequest.getFacetField().getPathList().toArray(new String[0]));
						handleFacetResult(builder, facetResult, countRequest);
					}
				}

			}
			else {
				is.search(q, collector);
			}

			ScoreDoc[] results = collector.topDocs().scoreDocs;

			int totalHits = collector.getTotalHits();

			builder.setTotalHits(totalHits);

			boolean moreAvailable = (results.length == hasMoreAmount);

			int numResults = Math.min(results.length, amount);

			for (int i = 0; i < numResults; i++) {
				ScoredResult.Builder srBuilder = handleDocResult(is, sortRequest, sorting, results, i);

				builder.addScoredResult(srBuilder.build());

			}

			if (moreAvailable) {
				ScoredResult.Builder srBuilder = handleDocResult(is, sortRequest, sorting, results, numResults);
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
		finally {
			if (ir != null) {
				ir.close();
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

	private ScoredResult.Builder handleDocResult(IndexSearcher is, SortRequest sortRequest, boolean sorting, ScoreDoc[] results, int i)
					throws CorruptIndexException, IOException {
		int docId = results[i].doc;
		Document d = is.doc(docId, fetchSet);
		ScoredResult.Builder srBuilder = ScoredResult.newBuilder();
		srBuilder.setScore(results[i].score);
		srBuilder.setUniqueId(d.get(indexConfig.getUniqueIdField()));

		IndexableField f = d.getField(LumongoConstants.TIMESTAMP_FIELD);
		srBuilder.setTimestamp(f.numericValue().longValue());

		srBuilder.setDocId(docId);
		srBuilder.setSegment(segmentNumber);
		srBuilder.setIndexName(indexName);
		srBuilder.setResultIndex(i);
		if (sorting) {
			FieldDoc result = (FieldDoc) results[i];

			int c = 0;
			for (Object o : result.fields) {
				FieldSort fieldSort = sortRequest.getFieldSort(c);
				String sortField = fieldSort.getSortField();
				if (indexConfig.isNumericOrDateField(sortField)) {
					if (indexConfig.isNumericIntField(sortField)) {
						if (o == null) {
							srBuilder.addSortInteger(0); // TODO what should nulls value be?
						}
						else {
							srBuilder.addSortInteger((Integer) o);
						}
					}
					else if (indexConfig.isNumericLongField(sortField)) {
						if (o == null) {
							srBuilder.addSortLong(0L);// TODO what should nulls value be?
						}
						else {
							srBuilder.addSortLong((Long) o);
						}
					}
					else if (indexConfig.isNumericFloatField(sortField)) {
						if (o == null) {
							srBuilder.addSortFloat(0f);// TODO what should nulls value be?
							// value be?
						}
						else {
							srBuilder.addSortFloat((Float) o);
						}
					}
					else if (indexConfig.isNumericDoubleField(sortField)) {
						if (o == null) {
							srBuilder.addSortDouble(0);// TODO what should nulls value be?
						}
						else {
							srBuilder.addSortDouble((Double) o);
						}
					}
					else if (indexConfig.isDateField(sortField)) {
						if (o == null) {
							srBuilder.addSortDate(0L);// TODO what should nulls value be?
						}
						else {
							srBuilder.addSortDate((Long) o);
						}
					}
				}

				else {
					if (o == null) {
						srBuilder.addSortTerm(""); // TODO what should nulls value be?
					}
					else {
						BytesRef b = (BytesRef) o;
						srBuilder.addSortTerm(b.utf8ToString());
					}
				}

				c++;
			}
		}
		return srBuilder;
	}
	
	private void possibleCommit() throws IOException {
		lastChange = System.currentTimeMillis();

		long count = counter.incrementAndGet();
		if ((count % indexConfig.getSegmentCommitInterval()) == 0) {
			forceCommit();
		}
		else if ((count % indexConfig.getSegmentFlushInterval()) == 0) {
			taxonomyWriter.flush();
			indexWriter.flush(indexConfig.getApplyUncommitedDeletes());
		}
		if (queryCacheEnabled) {
			queryResultCacheRealtime.clear();
		}
	}
	
	public void forceCommit() throws IOException {
		long currentTime = System.currentTimeMillis();

		taxonomyWriter.commit();
		indexWriter.commit();

		if (queryCacheEnabled) {
			queryResultCacheRealtime.clear();
			queryResultCache.clear();
		}

		lastCommit = currentTime;

	}
	
	public void doCommit() throws IOException {

		long currentTime = System.currentTimeMillis();

		Long lastCh = lastChange;
		// if changes since started

		if (lastCh != null) {
			if ((currentTime - lastCh) > (indexConfig.getIdleTimeWithoutCommit() * 1000)) {
				if ((lastCommit == null) || (lastCh > lastCommit)) {
					log.info("Flushing segment <" + segmentNumber + "> for index <" + indexName + ">");
					forceCommit();
				}
			}
		}
	}
	
	public void close() throws IOException {
		forceCommit();

		taxonomyWriter.close();
		indexWriter.close();
	}

	public void index(String uniqueId, BSONObject document, long timestamp) throws Exception {
		Document d = new Document();

		List<FacetField> facetFields = new ArrayList<>();
		for (String storedFieldName : indexConfig.getIndexedStoredFieldNames()) {

			FieldConfig fc = indexConfig.getFieldConfig(storedFieldName);

			if (fc != null) {

				Object o = getValueFromDocument(document, storedFieldName);

				if (o != null) {
					handleFacetsForStoredField(facetFields, fc, o);

					for (IndexAs indexAs : fc.getIndexAsList()) {
						String indexedFieldName = indexAs.getIndexFieldName();
						LMAnalyzer indexFieldAnalyzer = indexAs.getAnalyzer();
						if (LMAnalyzer.NUMERIC_INT.equals(indexFieldAnalyzer)) {
							IntFieldIndexer.INSTANCE.index(d, storedFieldName, o, indexedFieldName);
						}
						else if (LMAnalyzer.NUMERIC_LONG.equals(indexFieldAnalyzer)) {
							LongFieldIndexer.INSTANCE.index(d, storedFieldName, o, indexedFieldName);
						}
						else if (LMAnalyzer.NUMERIC_FLOAT.equals(indexFieldAnalyzer)) {
							FloatFieldIndexer.INSTANCE.index(d, storedFieldName, o, indexedFieldName);
						}
						else if (LMAnalyzer.NUMERIC_DOUBLE.equals(indexFieldAnalyzer)) {
							DoubleFieldIndexer.INSTANCE.index(d, storedFieldName, o, indexedFieldName);
						}
						else if (LMAnalyzer.DATE.equals(indexFieldAnalyzer)) {
							DateFieldIndexer.INSTANCE.index(d, storedFieldName, o, indexedFieldName);
						}
						else {
							StringFieldIndexer.INSTANCE.index(d, storedFieldName, o, indexedFieldName);
						}
					}
				}
			}

		}

		if (!facetFields.isEmpty()) {

			for (FacetField ff : facetFields) {
				d.add(ff);
			}
			d = facetsConfig.build(taxonomyWriter, d);

		}

		d.removeFields(indexConfig.getUniqueIdField());
		d.add(new TextField(indexConfig.getUniqueIdField(), uniqueId, Store.NO));

		// make sure the update works because it is searching on a term
		d.add(new StringField(indexConfig.getUniqueIdField(), uniqueId, Store.YES));

		d.add(new LongField(LumongoConstants.TIMESTAMP_FIELD, timestamp, Store.YES));

		Term term = new Term(indexConfig.getUniqueIdField(), uniqueId);
		indexWriter.updateDocument(term, d);
		possibleCommit();
	}

	private void handleFacetsForStoredField(List<FacetField> facetFields, FieldConfig fc, Object o) throws Exception {
		for (FacetAs fa : fc.getFacetAsList()) {
			if (LMFacetType.STANDARD.equals(fa.getFacetType())) {
				if (o instanceof Collection) {
					Collection<?> c = (Collection<?>) o;
					for (Object obj : c) {
						facetFields.add(new FacetField(fa.getFacetName(), obj.toString()));
					}

				}
				else if (o instanceof Object[]) {
					Object[] arr = (Object[]) o;
					for (Object obj : arr) {
						facetFields.add(new FacetField(fa.getFacetName(), obj.toString()));
					}
				}
				else {
					facetFields.add(new FacetField(fa.getFacetName(), o.toString()));
				}
			}
			else if (LMFacetType.DATE_YYYY_MM_DD.equals(fa.getFacetType())) {
				if (o instanceof Date) {
					Date da = (Date) o;
					DateTime dt = new DateTime(da);
					facetFields.add(new FacetField(fa.getFacetName(), String.format("%04d", dt.getYear()), String.format("%02d", dt.getMonthOfYear()),
									String.format("%02d", dt.getDayOfMonth())));
				}
				else if (o instanceof Collection) {
					Collection<?> c = (Collection<?>) o;
					for (Object obj : c) {
						if (obj instanceof Date) {
							Date da = (Date) o;
							DateTime dt = new DateTime(da);
							facetFields.add(new FacetField(fa.getFacetName(), String.format("%04d", dt.getYear()), String.format("%02d", dt.getMonthOfYear()),
											String.format("%02d", dt.getDayOfMonth())));
						}
						else {
							throw new Exception(
											"Cannot facet date for field <" + fc.getStoredFieldName() + ">: excepted collection of Date, found Collection of <"
															+ obj.getClass().getSimpleName() + ">");
						}
					}
				}
				else {
					throw new Exception(
									"Cannot facet date for field <" + fc.getStoredFieldName() + ">: excepted Date or Collection of Date, found <" + o.getClass()
													.getSimpleName() + ">");
				}
			}
			else if (LMFacetType.DATE_YYYYMMDD.equals(fa.getFacetType())) {
				if (o instanceof Date) {
					Date da = (Date) o;
					DateTime dt = new DateTime(da).withZone(DateTimeZone.UTC);
					String facetValue = FORMATTER_YYYY_MM_DD.print(dt);
					facetFields.add(new FacetField(fa.getFacetName(), facetValue));
				}
				else if (o instanceof Collection) {
					Collection<?> c = (Collection<?>) o;
					for (Object obj : c) {
						if (obj instanceof Date) {
							Date da = (Date) o;
							DateTime dt = new DateTime(da);
							facetFields.add(new FacetField(fa.getFacetName(), dt.getYear() + "" + dt.getMonthOfYear() + "" + dt.getDayOfMonth() + ""));
						}
						else {
							throw new Exception(
											"Cannot facet date for field <" + fc.getStoredFieldName() + ">: excepted collection of Date, found Collection of <"
															+ obj.getClass().getSimpleName() + ">");
						}
					}
				}
				else {
					throw new Exception(
									"Cannot facet date for field <" + fc.getStoredFieldName() + ">: excepted Date or Collection of Date, found <" + o.getClass()
													.getSimpleName() + ">");
				}
			}

		}
	}

	public void deleteDocument(String uniqueId) throws Exception {
		Term term = new Term(uniqueIdField, uniqueId);
		indexWriter.deleteDocuments(term);
		possibleCommit();

	}

	public void optimize() throws CorruptIndexException, IOException {
		lastChange = System.currentTimeMillis();
		indexWriter.forceMerge(1);
		forceCommit();
	}

	public GetFieldNamesResponse getFieldNames() throws CorruptIndexException, IOException {
		GetFieldNamesResponse.Builder builder = GetFieldNamesResponse.newBuilder();

		DirectoryReader ir = DirectoryReader.open(indexWriter, indexConfig.getApplyUncommitedDeletes());

		Set<String> fields = new HashSet<String>();

		for (LeafReaderContext subreaderContext : ir.leaves()) {
			FieldInfos fieldInfos = subreaderContext.reader().getFieldInfos();
			for (FieldInfo fi : fieldInfos) {
				String fieldName = fi.name;
				fields.add(fieldName);
			}
		}

		for (String fieldName : fields) {
			builder.addFieldName(fieldName);
		}

		return builder.build();
	}

	public void clear() throws IOException {
		// index has write lock so none needed here
		indexWriter.deleteAll();
		forceCommit();
	}

	public GetTermsResponse getTerms(GetTermsRequest request) throws IOException {
		GetTermsResponse.Builder builder = GetTermsResponse.newBuilder();

		DirectoryReader ir = null;
		try {
			ir = indexWriter.getReader(indexConfig.getApplyUncommitedDeletes(), request.getRealTime());

			String fieldName = request.getFieldName();
			String startTerm = "";

			if (request.hasStartingTerm()) {
				startTerm = request.getStartingTerm();
			}
			
			Pattern termFilter = null;
			if (request.hasTermFilter()) {
				termFilter = Pattern.compile(request.getTermFilter());
			}
			
			Pattern termMatch = null;
			if (request.hasTermMatch()) {
				termMatch = Pattern.compile(request.getTermMatch());
			}
			
			BytesRef startTermBytes = new BytesRef(startTerm);

			SortedMap<String, AtomicLong> termsMap = new TreeMap<String, AtomicLong>();

			for (LeafReaderContext subreaderContext : ir.leaves()) {
				Fields fields = subreaderContext.reader().fields();
				if (fields != null) {

					Terms terms = fields.terms(fieldName);
					if (terms != null) {
						// TODO reuse?
						TermsEnum termsEnum = terms.iterator(null);
						SeekStatus seekStatus = termsEnum.seekCeil(startTermBytes);

						BytesRef text;
						if (!seekStatus.equals(SeekStatus.END)) {
							text = termsEnum.term();

							handleTerm(termsMap, termsEnum, text, termFilter, termMatch);

							while ((text = termsEnum.next()) != null) {
								handleTerm(termsMap, termsEnum, text, termFilter, termMatch);
							}

						}
					}
				}

			}

			int amount = Math.min(request.getAmount(), termsMap.size());

			int i = 0;
			for (String term : termsMap.keySet()) {
				AtomicLong docFreq = termsMap.get(term);
				builder.addTerm(Lumongo.Term.newBuilder().setValue(term).setDocFreq(docFreq.get()));
				
				// TODO remove the limit and paging and just return all?
				i++;
				if (i > amount) {
					break;
				}

			}

			return builder.build();
		}
		finally {
			if (ir != null) {
				ir.close();
			}
		}
	}

	private void handleTerm(SortedMap<String, AtomicLong> termsMap, TermsEnum termsEnum, BytesRef text, Pattern termFilter, Pattern termMatch)
					throws IOException {
		String textStr = text.utf8ToString();

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

		if (!termsMap.containsKey(textStr)) {
			termsMap.put(textStr, new AtomicLong());
		}
		termsMap.get(textStr).addAndGet(termsEnum.docFreq());
	}

	public SegmentCountResponse getNumberOfDocs(boolean realTime) throws CorruptIndexException, IOException {
		IndexReader ir = null;

		try {
			ir = indexWriter.getReader(indexConfig.getApplyUncommitedDeletes(), realTime);
			int count = ir.numDocs();
			return SegmentCountResponse.newBuilder().setNumberOfDocs(count).setSegmentNumber(segmentNumber).build();
		}
		finally {
			if (ir != null) {
				ir.close();
			}
		}
	}

}
