package org.lumongo.server.indexing;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ReaderUtil;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;
import org.lumongo.cluster.message.Lumongo.GetTermsRequest;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMField;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.cluster.message.Lumongo.SegmentCountResponse;
import org.lumongo.cluster.message.Lumongo.SegmentResponse;
import org.lumongo.server.config.IndexConfig;

public class Segment {
	private final static Logger log = Logger.getLogger(Segment.class);
	
	private final int segmentNumber;
	private final IndexWriter indexWriter;
	private final IndexConfig indexConfig;
	
	private final String uniqueIdField;
	private final FieldSelector uniqueIdOnlyFieldSelector;
	
	private final AtomicLong counter;
	
	private Long lastCommit;
	private Long lastChange;
	private String indexName;
	
	public Segment(int segmentNumber, IndexWriter indexWriter, IndexConfig indexConfig) {
		this.segmentNumber = segmentNumber;
		this.indexWriter = indexWriter;
		this.indexConfig = indexConfig;
		
		this.uniqueIdField = indexConfig.getUniqueIdField();
		
		this.counter = new AtomicLong();
		this.lastCommit = null;
		this.lastChange = null;
		this.indexName = indexConfig.getIndexName();
		
		// this is probably unnecessary since only the unique id is being stored
		this.uniqueIdOnlyFieldSelector = new FieldSelector() {
			
			private static final long serialVersionUID = 1L;
			
			@Override
			public FieldSelectorResult accept(String fieldName) {
				return (uniqueIdField.equals(fieldName) ? FieldSelectorResult.LOAD : FieldSelectorResult.NO_LOAD);
			}
		};
		
	}
	
	public int getSegmentNumber() {
		return segmentNumber;
	}
	
	public SegmentResponse querySegment(Query q, int amount, ScoreDoc after) throws Exception {
		
		IndexReader ir = IndexReader.open(indexWriter, indexConfig.getApplyUncommitedDeletes());
		
		IndexSearcher is = new IndexSearcher(ir);
		
		Weight w = is.createNormalizedWeight(q);
		boolean docsScoredInOrder = !w.scoresDocsOutOfOrder();
		
		int checkForMore = amount + 1;
		
		TopScoreDocCollector collector = TopScoreDocCollector.create(checkForMore, after, docsScoredInOrder);
		is.search(w, null, collector);
		ScoreDoc[] results = collector.topDocs().scoreDocs;
		
		int totalHits = collector.getTotalHits();
		
		SegmentResponse.Builder builder = SegmentResponse.newBuilder();
		
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
	
	private void possibleCommit() throws CorruptIndexException, IOException {
		lastChange = System.currentTimeMillis();
		
		long count = counter.incrementAndGet();
		if (count % indexConfig.getSegmentCommitInterval() == 0) {
			forceCommit();
		}
	}
	
	public void forceCommit() throws CorruptIndexException, IOException {
		long currentTime = System.currentTimeMillis();
		indexWriter.commit();
		lastCommit = currentTime;
	}
	
	public void doCommit() throws CorruptIndexException, IOException {
		
		long currentTime = System.currentTimeMillis();
		
		Long lastCh = lastChange;
		// if changes since started
		
		if (lastCh != null) {
			if ((currentTime - lastCh) > indexConfig.getIdleTimeWithoutCommit()) {
				if (lastCommit == null || lastCh > lastCommit) {
					log.info("Flushing segment <" + segmentNumber + "> for index <" + indexName + ">");
					forceCommit();
				}
			}
		}
	}
	
	public void close() throws CorruptIndexException, IOException {
		forceCommit();
		indexWriter.close();
	}
	
	public void index(String uniqueId, LMDoc lmDoc) throws CorruptIndexException, IOException {
		Document d = new Document();
		
		for (LMField indexedField : lmDoc.getIndexedFieldList()) {
			String fieldName = indexedField.getFieldName();
			List<String> fieldValueList = indexedField.getFieldValueList();
			for (String fieldValue : fieldValueList) {
				d.add(new Field(fieldName, fieldValue, Store.NO, org.apache.lucene.document.Field.Index.ANALYZED));
			}
		}
		d.removeFields(indexConfig.getUniqueIdField());
		d.add(new Field(indexConfig.getUniqueIdField(), uniqueId, Store.NO, org.apache.lucene.document.Field.Index.ANALYZED));
		//make sure the update works
		d.add(new Field(indexConfig.getUniqueIdField(), uniqueId, Store.YES, org.apache.lucene.document.Field.Index.NOT_ANALYZED_NO_NORMS));
		
		Term term = new Term(indexConfig.getUniqueIdField(), uniqueId);
		indexWriter.updateDocument(term, d);
		
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
		
		IndexReader ir = IndexReader.open(indexWriter, indexConfig.getApplyUncommitedDeletes());
		
		String fieldName = request.getFieldName();
		String startTerm = "";
		
		if (request.hasStartingTerm()) {
			startTerm = request.getStartingTerm();
		}
		
		int amount = request.getAmount();
		
		Term start = new Term(fieldName, startTerm);
		TermEnum terms = ir.terms(start);
		while (fieldName.equals(terms.term().field()) && amount > builder.getValueCount()) {
			Term t = terms.term();
			String value = t.text();
			
			builder.addValue(value);
			
			if (!terms.next())
				break;
		}
		
		return builder.build();
	}
	
	public SegmentCountResponse getNumberOfDocs() throws CorruptIndexException, IOException {
		IndexReader ir = IndexReader.open(indexWriter, indexConfig.getApplyUncommitedDeletes());
		int count = ir.numDocs();
		return SegmentCountResponse.newBuilder().setNumberOfDocs(count).setSegmentNumber(segmentNumber).build();
	}
}
