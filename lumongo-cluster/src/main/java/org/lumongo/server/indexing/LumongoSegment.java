package org.lumongo.server.indexing;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
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
import org.lumongo.server.indexing.field.IndexWriterManager;
import org.lumongo.server.indexing.field.IntFieldIndexer;
import org.lumongo.server.indexing.field.LongFieldIndexer;
import org.lumongo.server.indexing.field.StringFieldIndexer;
import org.lumongo.server.searching.QueryWithFilters;
import org.lumongo.util.LumongoUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

	private final IndexConfig indexConfig;
	private final String uniqueIdField;
	private final AtomicLong counter;
	private final Set<String> fetchSet;
	private final IndexWriterManager indexWriterManager;

	private IndexWriter indexWriter;
	private DirectoryReader directoryReader;

	private Long lastCommit;
	private Long lastChange;
	private String indexName;
	private QueryResultCache queryResultCache;
	private FacetsConfig facetsConfig;

	private boolean queryCacheEnabled;

	private int segmentQueryCacheMaxAmount;

	public LumongoSegment(int segmentNumber, IndexWriterManager indexWriterManager, IndexConfig indexConfig) throws Exception {

		setupQueryCache(indexConfig);

		this.segmentNumber = segmentNumber;

		this.indexWriterManager = indexWriterManager;
		this.indexConfig = indexConfig;

		openIndexWriters();

		this.facetsConfig = getFacetsConfig();

		this.uniqueIdField = indexConfig.getUniqueIdField();

		this.fetchSet = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(uniqueIdField, LumongoConstants.TIMESTAMP_FIELD)));

		this.counter = new AtomicLong();
		this.lastCommit = null;
		this.lastChange = null;
		this.indexName = indexConfig.getIndexName();

	}

	private void reopenIndexWritersIfNecessary() throws Exception {
		if (!indexWriter.isOpen()) {
			synchronized (this) {
				if (!indexWriter.isOpen()) {
					this.indexWriter = this.indexWriterManager.getIndexWriter(segmentNumber);
					this.directoryReader = DirectoryReader.open(indexWriter, indexConfig.getApplyUncommitedDeletes());
				}
			}
		}

	}

	private void openIndexWriters() throws Exception {
		if (this.indexWriter != null) {
			indexWriter.close();
		}
		this.indexWriter = this.indexWriterManager.getIndexWriter(segmentNumber);
		this.directoryReader = DirectoryReader.open(indexWriter, indexConfig.getApplyUncommitedDeletes());
	}

	public static Object getValueFromDocument(BSONObject document, String storedFieldName) {

		Object o;
		if (storedFieldName.contains(".")) {
			o = document;
			String[] fields = storedFieldName.split("\\.");
			for (String field : fields) {
				if (o instanceof List) {
					List<?> list = (List<?>) o;
					List<Object> values = new ArrayList<>();
					list.stream().filter(item -> item instanceof BSONObject).forEach(item -> {
						BSONObject dbObj = (BSONObject) item;
						Object object = dbObj.get(field);
						if (object != null) {
							values.add(object);
						}
					});
					o = values;
				}
				else if (o instanceof BSONObject) {
					BSONObject dbObj = (BSONObject) o;
					o = dbObj.get(field);
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

	protected FacetsConfig getFacetsConfig() {
		FacetsConfig facetsConfig = new FacetsConfig();
		for (String storedFieldName : indexConfig.getIndexedStoredFieldNames()) {

			FieldConfig fc = indexConfig.getFieldConfig(storedFieldName);
			for (FacetAs fa : fc.getFacetAsList()) {
				facetsConfig.setMultiValued(fa.getFacetName(), true);
				facetsConfig.setIndexFieldName(fa.getFacetName(), FacetsConfig.DEFAULT_INDEX_FIELD_NAME + "." + fa.getFacetName());
			}

		}
		return facetsConfig;
	}

	private void setupQueryCache(IndexConfig indexConfig) {
		queryCacheEnabled = (indexConfig.getSegmentQueryCacheSize() > 0);
		segmentQueryCacheMaxAmount = indexConfig.getSegmentQueryCacheMaxAmount();

		if (queryCacheEnabled) {
			this.queryResultCache = new QueryResultCache(indexConfig.getSegmentQueryCacheSize(), 8);
		}
	}

	public void updateIndexSettings(IndexSettings indexSettings) throws Exception {

		this.indexConfig.configure(indexSettings);
		this.facetsConfig = getFacetsConfig();
		setupQueryCache(indexConfig);
		openIndexWriters();

	}

	public int getSegmentNumber() {
		return segmentNumber;
	}

	public SegmentResponse querySegment(QueryWithFilters queryWithFilters, int amount, FieldDoc after, FacetRequest facetRequest, SortRequest sortRequest,
					QueryCacheKey queryCacheKey) throws Exception {

		QueryResultCache qrc = queryResultCache;

		boolean useCache = queryCacheEnabled && ((segmentQueryCacheMaxAmount <= 0) || (segmentQueryCacheMaxAmount >= amount));
		if (useCache) {
			SegmentResponse cacheSegmentResponse = qrc.getCacheSegmentResponse(queryCacheKey);
			if (cacheSegmentResponse != null) {
				return cacheSegmentResponse;
			}

		}

		Query q = queryWithFilters.getQuery();

		if (!queryWithFilters.getFilterQueries().isEmpty()) {
			BooleanQuery booleanQuery = new BooleanQuery();

			for (Query filterQuery : queryWithFilters.getFilterQueries()) {
				booleanQuery.add(filterQuery, BooleanClause.Occur.MUST);
			}

			q = new QueryWrapperFilter(booleanQuery);
		}

		reopenIndexWritersIfNecessary();

		openReaderIfChanges();

		IndexSearcher indexSearcher = new IndexSearcher(directoryReader);

		int hasMoreAmount = amount + 1;

		TopDocsCollector<?> collector;

		List<SortField> sortFields = new ArrayList<>();
		boolean sorting = (sortRequest != null) && !sortRequest.getFieldSortList().isEmpty();
		if (sorting) {

			for (FieldSort fs : sortRequest.getFieldSortList()) {
				boolean reverse = Direction.DESCENDING.equals(fs.getDirection());

				String sortField = fs.getSortField();
				Lumongo.SortAs.SortType sortType = indexConfig.getSortType(sortField);

				if (IndexConfig.isNumericOrDateSortType(sortType)) {
					SortField.Type type;
					if (IndexConfig.isNumericIntSortType(sortType)) {
						type = SortField.Type.INT;
					}
					else if (IndexConfig.isNumericLongSortType(sortType)) {
						type = SortField.Type.LONG;
					}
					else if (IndexConfig.isNumericFloatSortType(sortType)) {
						type = SortField.Type.FLOAT;
					}
					else if (IndexConfig.isNumericDoubleSortType(sortType)) {
						type = SortField.Type.DOUBLE;
					}
					else if (IndexConfig.isNumericDateSortType(sortType)) {
						type = SortField.Type.LONG;
					}
					else {
						throw new Exception("Invalid numeric sort type <" + sortType + "> for sort field <" + sortField + ">");
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

			//TODO fix me

			if (facetRequest.getDrillSideways()) {
				DefaultSortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(directoryReader);
				DrillSideways ds = new DrillSideways(indexSearcher, facetsConfig, state);
				DrillSidewaysResult ddsr = ds.search((DrillDownQuery) q, collector);
				for (CountRequest countRequest : facetRequest.getCountRequestList()) {

					int maxFacets = (countRequest.getMaxFacets() * 2) + 32;

					FacetResult facetResult = ddsr.facets
									.getTopChildren(maxFacets, countRequest.getFacetField().getLabel());

					handleFacetResult(builder, facetResult, countRequest);

				}
			}
			else {

				FacetsCollector facetsCollector = new FacetsCollector();
				indexSearcher.search(q, MultiCollector.wrap(collector, facetsCollector));

				for (CountRequest countRequest : facetRequest.getCountRequestList()) {
					int maxFacets = (countRequest.getMaxFacets() * 2) + 32;
					String label = countRequest.getFacetField().getLabel();
					String indexFieldName = facetsConfig.getDimConfig(
									label).indexFieldName;
					DefaultSortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(directoryReader, indexFieldName);
					Facets facets = new SortedSetDocValuesFacetCounts(state, facetsCollector);
					FacetResult facetResult = facets
									.getTopChildren(maxFacets, label);
					handleFacetResult(builder, facetResult, countRequest);
				}
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
			ScoredResult.Builder srBuilder = handleDocResult(indexSearcher, sortRequest, sorting, results, i);

			builder.addScoredResult(srBuilder.build());

		}

		if (moreAvailable) {
			ScoredResult.Builder srBuilder = handleDocResult(indexSearcher, sortRequest, sorting, results, numResults);
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

	private void openReaderIfChanges() throws IOException {
		DirectoryReader newDirectoryReader = DirectoryReader.openIfChanged(directoryReader, indexWriter, indexConfig.getApplyUncommitedDeletes());
		if (newDirectoryReader != null) {
			directoryReader = newDirectoryReader;
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

	private ScoredResult.Builder handleDocResult(IndexSearcher is, SortRequest sortRequest, boolean sorting, ScoreDoc[] results, int i) throws IOException {
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

				Lumongo.SortAs.SortType sortType = indexConfig.getSortType(sortField);
				if (IndexConfig.isNumericOrDateSortType(sortType)) {
					if (IndexConfig.isNumericIntSortType(sortType)) {
						if (o == null) {
							srBuilder.addSortInteger(0); // TODO what should nulls value be?
						}
						else {
							srBuilder.addSortInteger((Integer) o);
						}
					}
					else if (IndexConfig.isNumericLongSortType(sortType)) {
						if (o == null) {
							srBuilder.addSortLong(0L);// TODO what should nulls value be?
						}
						else {
							srBuilder.addSortLong((Long) o);
						}
					}
					else if (IndexConfig.isNumericFloatSortType(sortType)) {
						if (o == null) {
							srBuilder.addSortFloat(0f);// TODO what should nulls value be?
							// value be?
						}
						else {
							srBuilder.addSortFloat((Float) o);
						}
					}
					else if (IndexConfig.isNumericDoubleSortType(sortType)) {
						if (o == null) {
							srBuilder.addSortDouble(0);// TODO what should nulls value be?
						}
						else {
							srBuilder.addSortDouble((Double) o);
						}
					}
					else if (IndexConfig.isNumericDateSortType(sortType)) {
						if (o == null) {
							srBuilder.addSortDate(0L);// TODO what should nulls value be?
						}
						else {
							srBuilder.addSortDate(((Long) o));
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

	}

	public void forceCommit() throws IOException {
		long currentTime = System.currentTimeMillis();

		indexWriter.commit();

		if (queryCacheEnabled) {
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

		indexWriter.close();
	}

	public void index(String uniqueId, BSONObject document, long timestamp) throws Exception {
		reopenIndexWritersIfNecessary();

		Document d = new Document();

		List<Field> facetFields = new ArrayList<>();
		for (String storedFieldName : indexConfig.getIndexedStoredFieldNames()) {

			FieldConfig fc = indexConfig.getFieldConfig(storedFieldName);

			if (fc != null) {

				Object o = getValueFromDocument(document, storedFieldName);

				if (o != null) {
					handleFacetsForStoredField(facetFields, fc, o);

					handleSortForStoredField(d, storedFieldName, fc, o);

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

			for (Field ff : facetFields) {
				d.add(ff);
			}
			d = facetsConfig.build(d);

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

	private void handleSortForStoredField(Document d, String storedFieldName, FieldConfig fc, Object o) {

		if (fc.hasSortAs()) {
			Lumongo.SortAs sortAs = fc.getSortAs();
			String sortFieldName = sortAs.getSortFieldName();

			if (IndexConfig.isNumericOrDateSortType(sortAs.getSortType())) {
				LumongoUtil.handleLists(o, obj -> {
					if (obj instanceof Number) {

						Number number = (Number) obj;
						SortedNumericDocValuesField docValue = null;
						if (Lumongo.SortAs.SortType.NUMERIC_INT.equals(sortAs.getSortType())) {
							docValue = new SortedNumericDocValuesField(sortFieldName, number.intValue());
						}
						else if (Lumongo.SortAs.SortType.NUMERIC_LONG.equals(sortAs.getSortType())) {
							docValue = new SortedNumericDocValuesField(sortFieldName, number.longValue());
						}
						else if (Lumongo.SortAs.SortType.NUMERIC_FLOAT.equals(sortAs.getSortType())) {
							docValue = new SortedNumericDocValuesField(sortFieldName, NumericUtils.floatToSortableInt(number.floatValue()));
						}
						else if (Lumongo.SortAs.SortType.NUMERIC_DOUBLE.equals(sortAs.getSortType())) {
							docValue = new SortedNumericDocValuesField(sortFieldName, NumericUtils.doubleToSortableLong(number.doubleValue()));
						}
						else {
							throw new RuntimeException("Not handled numeric sort type <" + sortAs.getSortType() + "> for document field <" + storedFieldName
											+ "> / sort field <" + sortFieldName + ">");
						}

						d.add(docValue);
					}
					else if (obj instanceof Date) {
						Date date = (Date) obj;
						SortedNumericDocValuesField docValue = new SortedNumericDocValuesField(sortFieldName, date.getTime());
						d.add(docValue);
					}
					else {
						throw new RuntimeException(
										"Expecting number for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o
														.getClass() + ">");
					}
				});
			}
			else if (Lumongo.SortAs.SortType.STRING.equals(sortAs.getSortType())) {
				LumongoUtil.handleLists(o, obj -> {
					SortedSetDocValuesField docValue = new SortedSetDocValuesField(sortFieldName, new BytesRef(o.toString()));
					d.add(docValue);
				});
			}
			else {
				throw new RuntimeException("Not handled sort type <" + sortAs.getSortType() + "> for document field <" + storedFieldName + "> / sort field <"
								+ sortFieldName + ">");
			}

		}
	}

	private void handleFacetsForStoredField(List<Field> facetFields, FieldConfig fc, Object o) throws Exception {
		for (FacetAs fa : fc.getFacetAsList()) {

			if (LMFacetType.STANDARD.equals(fa.getFacetType())) {
				LumongoUtil.handleLists(o, obj -> {
					String string = obj.toString();
					if (!string.isEmpty()) {
						facetFields.add(new SortedSetDocValuesFacetField(fa.getFacetName(), string));
					}
				});
			}
			else if (IndexConfig.isDateFacetType(fa.getFacetType())) {
				LumongoUtil.handleLists(o, obj -> {
					if (obj instanceof Date) {
						DateTime dt = new DateTime(obj).withZone(DateTimeZone.UTC);

						Field facetField;
						if (LMFacetType.DATE_YYYYMMDD.equals(fa.getFacetType())) {
							String facetValue = FORMATTER_YYYY_MM_DD.print(dt);
							facetField = new SortedSetDocValuesFacetField(fa.getFacetName(), facetValue);
						}
						else if (LMFacetType.DATE_YYYY_MM_DD.equals(fa.getFacetType())) {
							String date = String.format("%04d-%02d-%02d", dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth());
							facetField = new SortedSetDocValuesFacetField(fa.getFacetName(), date);
						}
						else {
							throw new RuntimeException("Not handled date facet type <" + fa.getFacetType() + "> for facet <" + fa.getFacetName() + ">");
						}

						facetFields.add(facetField);

					}
					else {
						throw new RuntimeException("Cannot facet date for document field <" + fc.getStoredFieldName() + "> / facet <" + fa.getFacetName()
										+ ">: excepted Date or Collection of Date, found <" + o.getClass().getSimpleName() + ">");
					}
				});
			}
			else {
				throw new Exception("Not handled facet type <" + fa.getFacetType() + "> for document field <" + fc.getStoredFieldName() + "> / facet <" + fa
								.getFacetName() + ">");
			}

		}
	}

	public void deleteDocument(String uniqueId) throws Exception {
		Term term = new Term(uniqueIdField, uniqueId);
		indexWriter.deleteDocuments(term);
		if (indexConfig.getApplyUncommitedDeletes()) {
			queryResultCache.clear();
		}
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

		for (LeafReaderContext subreaderContext : directoryReader.leaves()) {
			FieldInfos fieldInfos = subreaderContext.reader().getFieldInfos();
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

		SortedMap<String, AtomicLong> termsMap = new TreeMap<>();

		for (LeafReaderContext subreaderContext : directoryReader.leaves()) {
			Fields fields = subreaderContext.reader().fields();
			if (fields != null) {

				Terms terms = fields.terms(fieldName);
				if (terms != null) {
					// TODO reuse?
					TermsEnum termsEnum = terms.iterator();
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

	public SegmentCountResponse getNumberOfDocs(boolean realTime) throws IOException {

		openReaderIfChanges();
		int count = directoryReader.numDocs();
		return SegmentCountResponse.newBuilder().setNumberOfDocs(count).setSegmentNumber(segmentNumber).build();

	}

}
