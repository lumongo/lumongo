package org.lumongo.server.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.facet.search.FacetsCollector;
import org.apache.lucene.facet.search.params.CountFacetRequest;
import org.apache.lucene.facet.search.params.FacetSearchParams;
import org.apache.lucene.facet.search.results.FacetResult;
import org.apache.lucene.facet.search.results.FacetResultNode;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.directory.LumongoDirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.LumongoDirectoryTaxonomyWriter;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LumongoIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ReaderUtil;
import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.CountRequest;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FacetRequest;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;
import org.lumongo.cluster.message.Lumongo.GetTermsRequest;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMField;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.cluster.message.Lumongo.SegmentCountResponse;
import org.lumongo.cluster.message.Lumongo.SegmentResponse;
import org.lumongo.server.config.IndexConfig;

public class Segment {
	
	private final static Logger log = Logger.getLogger(Segment.class);
	
	private final int segmentNumber;
	
	private final LumongoIndexWriter indexWriter;
	private LumongoDirectoryTaxonomyWriter taxonomyWriter;
	private LumongoDirectoryTaxonomyReader taxonomyReader;
	
	private final IndexConfig indexConfig;
	
	private final String uniqueIdField;
	private final FieldSelector uniqueIdOnlyFieldSelector;
	
	private final AtomicLong counter;
	
	private Long lastCommit;
	private Long lastChange;
	private String indexName;
	private Analyzer analyzer;
	
	public Segment(int segmentNumber, LumongoIndexWriter indexWriter, LumongoDirectoryTaxonomyWriter taxonomyWriter, IndexConfig indexConfig, Analyzer analyzer)
			throws IOException {
		this.segmentNumber = segmentNumber;
		this.indexWriter = indexWriter;
		
		this.taxonomyWriter = taxonomyWriter;
		if (this.taxonomyWriter != null) {
			this.taxonomyReader = new LumongoDirectoryTaxonomyReader(taxonomyWriter);
		}
		
		this.indexConfig = indexConfig;
		this.analyzer = analyzer;
		
		this.uniqueIdField = indexConfig.getUniqueIdField();
		
		this.counter = new AtomicLong();
		this.lastCommit = null;
		this.lastChange = null;
		this.indexName = indexConfig.getIndexName();
		
		// this is probably unnecessary since only the unique id is being stored
		this.uniqueIdOnlyFieldSelector = new SingleFieldSelector(uniqueIdField);
		
	}
	
	public void updateIndexSettings(IndexSettings indexSettings, Analyzer analyzer) {
		this.analyzer = analyzer;
		this.indexConfig.configure(indexSettings);
	}
	
	public int getSegmentNumber() {
		return segmentNumber;
	}
	
	public SegmentResponse querySegment(Query q, int amount, ScoreDoc after, FacetRequest facetRequest, boolean realTime) throws Exception {
		
		IndexReader ir = null;
		
		try {
			
			//ir = IndexReader.open(indexWriter, indexConfig.getApplyUncommitedDeletes());
			//ir = IndexReader.open(indexWriter.getDirectory());
			ir = indexWriter.getReader(indexConfig.getApplyUncommitedDeletes(), realTime);
			
			IndexSearcher is = new IndexSearcher(ir);
			
			Weight w = is.createNormalizedWeight(q);
			boolean docsScoredInOrder = !w.scoresDocsOutOfOrder();
			
			int checkForMore = amount + 1;
			
			TopScoreDocCollector collector = TopScoreDocCollector.create(checkForMore, after, docsScoredInOrder);
			
			SegmentResponse.Builder builder = SegmentResponse.newBuilder();
			
			if (indexConfig.isFaceted()) {
				taxonomyReader.refresh(realTime);
				FacetSearchParams facetSearchParams = new FacetSearchParams();
				
				for (CountRequest count : facetRequest.getCountRequestList()) {
					facetSearchParams.addFacetRequest(new CountFacetRequest(new CategoryPath(count.getFacet(), LumongoConstants.FACET_DELIMITER), count
							.getMaxFacets()));
				}
				FacetsCollector facetsCollector = new FacetsCollector(facetSearchParams, ir, taxonomyReader);
				is.search(q, MultiCollector.wrap(collector, facetsCollector));
				
				List<FacetResult> facetResults = facetsCollector.getFacetResults();
				
				for (FacetResult fc : facetResults) {
					for (FacetResultNode subResult : fc.getFacetResultNode().getSubResults()) {
						FacetCount.Builder facetCountBuilder = FacetCount.newBuilder();
						CategoryPath cp = subResult.getLabel();
						long count = (long) subResult.getValue();
						facetCountBuilder.setCount(count);
						facetCountBuilder.setFacet(cp.toString(LumongoConstants.FACET_DELIMITER));
						builder.addFacetCount(facetCountBuilder);
					}
				}
			}
			else {
				is.search(w, null, collector);
			}
			
			ScoreDoc[] results = collector.topDocs().scoreDocs;
			
			int totalHits = collector.getTotalHits();
			
			builder.setTotalHits(totalHits);
			
			boolean moreAvailable = (results.length == checkForMore);
			
			int numResults = Math.min(results.length, amount);
			
			for (int i = 0; i < numResults; i++) {
				int docId = results[i].doc;
				Document d = is.doc(docId, uniqueIdOnlyFieldSelector);
				ScoredResult.Builder srBuilder = ScoredResult.newBuilder();
				srBuilder.setScore(results[i].score);
				srBuilder.setUniqueId(d.get(indexConfig.getUniqueIdField()));
				srBuilder.setDocId(docId);
				srBuilder.setSegment(segmentNumber);
				srBuilder.setIndexName(indexName);
				builder.addScoredResult(srBuilder.build());
				
			}
			
			builder.setMoreAvailable(moreAvailable);
			builder.setIndexName(indexName);
			builder.setSegmentNumber(segmentNumber);
			return builder.build();
		}
		finally {
			if (ir != null) {
				ir.close();
			}
			
		}
		
	}
	
	private void possibleCommit() throws CorruptIndexException, IOException {
		lastChange = System.currentTimeMillis();
		
		long count = counter.incrementAndGet();
		if (count % indexConfig.getSegmentCommitInterval() == 0) {
			forceCommit();
		}
		else if (count % indexConfig.getSegmentFlushInterval() == 0) {
			if (indexConfig.isFaceted()) {
				taxonomyWriter.flush();
			}
			indexWriter.flush(indexConfig.getApplyUncommitedDeletes());
		}
	}
	
	public void forceCommit() throws CorruptIndexException, IOException {
		long currentTime = System.currentTimeMillis();
		if (indexConfig.isFaceted()) {
			taxonomyWriter.commit();
		}
		
		indexWriter.commit();
		lastCommit = currentTime;
		
	}
	
	public void doCommit() throws CorruptIndexException, IOException {
		
		long currentTime = System.currentTimeMillis();
		
		Long lastCh = lastChange;
		// if changes since started
		
		if (lastCh != null) {
			if ((currentTime - lastCh) > (indexConfig.getIdleTimeWithoutCommit() * 1000)) {
				if (lastCommit == null || lastCh > lastCommit) {
					log.info("Flushing segment <" + segmentNumber + "> for index <" + indexName + ">");
					forceCommit();
				}
			}
		}
	}
	
	public void close() throws CorruptIndexException, IOException {
		forceCommit();
		if (indexConfig.isFaceted()) {
			taxonomyWriter.close();
		}
		
		indexWriter.close();
	}
	
	public void index(String uniqueId, LMDoc lmDoc) throws CorruptIndexException, IOException {
		Document d = new Document();
		
		for (LMField indexedField : lmDoc.getIndexedFieldList()) {
			String fieldName = indexedField.getFieldName();
			
			if (!indexConfig.isNumericField(fieldName)) {
				List<String> fieldValueList = indexedField.getFieldValueList();
				for (String fieldValue : fieldValueList) {
					d.add(new Field(fieldName, fieldValue, Store.NO, org.apache.lucene.document.Field.Index.ANALYZED));
				}
			}
			else {
				if (indexConfig.isNumericIntField(fieldName)) {
					List<Integer> valueList = indexedField.getIntValueList();
					for (int value : valueList) {
						d.add(new NumericField(fieldName).setIntValue(value));
					}
				}
				else if (indexConfig.isNumericLongField(fieldName)) {
					List<Long> valueList = indexedField.getLongValueList();
					for (long value : valueList) {
						d.add(new NumericField(fieldName).setLongValue(value));
					}
				}
				else if (indexConfig.isNumericFloatField(fieldName)) {
					List<Float> valueList = indexedField.getFloatValueList();
					for (float value : valueList) {
						d.add(new NumericField(fieldName).setFloatValue(value));
					}
				}
				else if (indexConfig.isNumericDoubleField(fieldName)) {
					List<Double> valueList = indexedField.getDoubleValueList();
					for (double value : valueList) {
						d.add(new NumericField(fieldName).setDoubleValue(value));
					}
				}
				else {
					//should be impossible
					throw new RuntimeException("Unsupported numeric field type for field <" + fieldName + ">");
				}
				
			}
		}
		d.removeFields(indexConfig.getUniqueIdField());
		d.add(new Field(indexConfig.getUniqueIdField(), uniqueId, Store.NO, org.apache.lucene.document.Field.Index.ANALYZED));
		
		//make sure the update works because it is searching on a term
		d.add(new Field(indexConfig.getUniqueIdField(), uniqueId, Store.YES, org.apache.lucene.document.Field.Index.NOT_ANALYZED_NO_NORMS));
		
		if (indexConfig.isFaceted()) {
			List<CategoryPath> categories = new ArrayList<CategoryPath>();
			for (String facet : lmDoc.getFacetList()) {
				categories.add(new CategoryPath(facet, LumongoConstants.FACET_DELIMITER));
			}
			CategoryDocumentBuilder categoryDocBuilder = new CategoryDocumentBuilder(taxonomyWriter);
			categoryDocBuilder.setCategoryPaths(categories);
			categoryDocBuilder.build(d);
		}
		else {
			if (lmDoc.getFacetCount() != 0) {
				throw new IOException("Cannot store facets into a non faceted index");
			}
		}
		
		Term term = new Term(indexConfig.getUniqueIdField(), uniqueId);
		indexWriter.updateDocument(term, d, analyzer);
		
		possibleCommit();
	}
	
	public void delete(String uniqueId) throws CorruptIndexException, IOException {
		Term term = new Term(uniqueIdField, uniqueId);
		indexWriter.deleteDocuments(term);
		possibleCommit();
	}
	
	@SuppressWarnings("deprecation")
	public void optimize() throws CorruptIndexException, IOException {
		lastChange = System.currentTimeMillis();
		indexWriter.optimize();
		forceCommit();
	}
	
	public GetFieldNamesResponse getFieldNames() throws CorruptIndexException, IOException {
		GetFieldNamesResponse.Builder builder = GetFieldNamesResponse.newBuilder();
		
		IndexReader ir = IndexReader.open(indexWriter, indexConfig.getApplyUncommitedDeletes());
		FieldInfos fieldInfos = ReaderUtil.getMergedFieldInfos(ir);
		
		for (FieldInfo fi : fieldInfos) {
			String fieldName = fi.name;
			builder.addFieldName(fieldName);
		}
		
		return builder.build();
	}
	
	public void clear() throws IOException {
		indexWriter.deleteAll();
		forceCommit();
		
	}
	
	public GetTermsResponse getTerms(GetTermsRequest request) throws IOException {
		GetTermsResponse.Builder builder = GetTermsResponse.newBuilder();
		
		IndexReader ir = null;
		try {
			ir = indexWriter.getReader(indexConfig.getApplyUncommitedDeletes(), request.getRealTime());
			
			String fieldName = request.getFieldName();
			String startTerm = "";
			
			if (request.hasStartingTerm()) {
				startTerm = request.getStartingTerm();
			}
			
			int amount = request.getAmount();
			
			Term start = new Term(fieldName, startTerm);
			TermEnum terms = ir.terms(start);
			if (terms.term() != null) {
				
				while (fieldName.equals(terms.term().field()) && amount > builder.getTermCount()) {
					Term t = terms.term();
					String value = t.text();
					int docFreq = terms.docFreq();
					
					builder.addTerm(Lumongo.Term.newBuilder().setValue(value).setDocFreq(docFreq));
					
					if (!terms.next())
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
