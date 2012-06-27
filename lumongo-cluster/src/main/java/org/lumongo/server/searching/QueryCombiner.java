package org.lumongo.server.searching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.IndexSegmentResponse;
import org.lumongo.cluster.message.Lumongo.InternalQueryResponse;
import org.lumongo.cluster.message.Lumongo.LastIndexResult;
import org.lumongo.cluster.message.Lumongo.LastResult;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.cluster.message.Lumongo.SegmentResponse;
import org.lumongo.server.indexing.Index;

public class QueryCombiner {
	
	private final static Logger log = Logger.getLogger(QueryCombiner.class);
	
	private final Map<String, Index> usedIndexMap;
	private final List<InternalQueryResponse> responses;
	
	private final Map<String, Map<Integer, SegmentQueryResult>> indexToSegmentResponseMap;
	private final List<SegmentQueryResult> segmentQueryResults;
	
	private final int amount;
	private final LastResult lastResult;
	
	private boolean isShort;
	private List<ScoredResult> results;
	private int resultsSize;
	
	public QueryCombiner(Map<String, Index> usedIndexMap, int amount, List<InternalQueryResponse> responses, LastResult lastResult) {
		this.usedIndexMap = usedIndexMap;
		this.responses = responses;
		this.amount = amount;
		this.indexToSegmentResponseMap = new HashMap<String, Map<Integer, SegmentQueryResult>>();
		this.segmentQueryResults = new ArrayList<SegmentQueryResult>();
		this.lastResult = lastResult;
		this.isShort = false;
		this.results = Collections.emptyList();
		this.resultsSize = 0;
	}
	
	public void validate() throws Exception {
		for (InternalQueryResponse iqr : responses) {
			
			for (IndexSegmentResponse isr : iqr.getIndexSegmentResponseList()) {
				String indexName = isr.getIndexName();
				if (!indexToSegmentResponseMap.containsKey(indexName)) {
					indexToSegmentResponseMap.put(indexName, new HashMap<Integer, SegmentQueryResult>());
				}
				
				for (SegmentResponse sr : isr.getSegmentReponseList()) {
					int segmentNumber = sr.getSegmentNumber();
					
					Map<Integer, SegmentQueryResult> segmentResponseMap = indexToSegmentResponseMap.get(indexName);
					
					if (segmentResponseMap.containsKey(segmentNumber)) {
						throw new Exception("Segment <" + segmentNumber + "> is repeated for <" + indexName + ">");
					}
					else {
						SegmentQueryResult sqr = new SegmentQueryResult(sr);
						segmentResponseMap.put(segmentNumber, sqr);
						segmentQueryResults.add(sqr);
					}
				}
				
			}
			
		}
		
		for (String indexName : usedIndexMap.keySet()) {
			int numberOfSegments = usedIndexMap.get(indexName).getNumberOfSegments();
			Map<Integer, SegmentQueryResult> segmentResponseMap = indexToSegmentResponseMap.get(indexName);
			
			if (segmentResponseMap == null) {
				throw new Exception("Missing index <" + indexName + "> in response");
			}
			
			if (segmentResponseMap.size() != numberOfSegments) {
				throw new Exception("Found <" + segmentResponseMap.size() + "> expected <" + numberOfSegments + ">");
			}
			
			for (int segmentNumber = 0; segmentNumber < numberOfSegments; segmentNumber++) {
				if (!segmentResponseMap.containsKey(segmentNumber)) {
					throw new Exception("Missing segment <" + segmentNumber + ">");
				}
			}
		}
	}
	
	public QueryResponse getQueryResponse() {
		long totalHits = 0;
		long returnedHits = 0;
		for (SegmentQueryResult sr : segmentQueryResults) {
			totalHits += sr.getTotalHits();
			returnedHits += sr.getReturnedHits();
		}
		
		QueryResponse.Builder builder = QueryResponse.newBuilder();
		builder.setTotalHits(totalHits);
		
		resultsSize = Math.min(amount, (int) returnedHits);
		
		results = new ArrayList<ScoredResult>(resultsSize);
		
		Map<String, ScoredResult[]> lastIndexResultMap = new HashMap<String, ScoredResult[]>();
		
		for (String indexName : indexToSegmentResponseMap.keySet()) {
			int numberOfSegments = usedIndexMap.get(indexName).getNumberOfSegments();
			lastIndexResultMap.put(indexName, new ScoredResult[numberOfSegments]);
		}
		
		for (LastIndexResult lir : lastResult.getLastIndexResultList()) {
			ScoredResult[] lastForSegmentArr = lastIndexResultMap.get(lir.getIndexName());
			// initialize with last results
			for (ScoredResult sr : lir.getLastForSegmentList()) {
				lastForSegmentArr[sr.getSegment()] = sr;
			}
		}
		
		Map<String, AtomicLong> totalFacetCounts = new HashMap<String, AtomicLong>();
		for (SegmentQueryResult sr : segmentQueryResults) {
			for (FacetCount fc : sr.getFacetCountList()) {
				String facet = fc.getFacet();
				if (!totalFacetCounts.containsKey(facet)) {
					totalFacetCounts.put(facet, new AtomicLong());
				}
				totalFacetCounts.get(facet).addAndGet(fc.getCount());
			}
		}
		for (String facet : totalFacetCounts.keySet()) {
			AtomicLong count = totalFacetCounts.get(facet);
			builder.addFacetCount(FacetCount.newBuilder().setFacet(facet).setCount(count.get()));
		}
		
		while (results.size() < resultsSize && !isShort) {
			float maxScore = Float.MIN_VALUE;
			SegmentQueryResult maxResult = null;
			
			boolean anyEmpty = false;
			for (SegmentQueryResult sr : segmentQueryResults) {
				if (!sr.hasNext() && sr.moreAvailable()) {
					anyEmpty = true;
					break;
				}
			}
			
			if (!anyEmpty) {
				for (SegmentQueryResult sr : segmentQueryResults) {
					if (sr.hasNext()) {
						
						ScoredResult sd = sr.previewNext();
						if (sd != null) {
							boolean notNearEnd = ((sr.getIndex() + 2) < sr.getReturnedHits());
							if (maxResult == null || ((sd.getScore() > maxScore) && notNearEnd)) {
								maxScore = sd.getScore();
								maxResult = sr;
							}
							else if (sr.getIndex() < maxResult.getIndex()) {
								
								if (maxResult.getIndexName().equals(sd.getIndexName())) {
									double segmentTolerance = usedIndexMap.get(maxResult.getIndexName()).getSegmentTolerance();
									double diff = (Math.abs(sd.getScore() - maxScore));
									
									if (diff < segmentTolerance) {
										maxScore = sd.getScore();
										maxResult = sr;
									}
								}
							}
							
						}
						
					}
				}
			}
			
			if (maxResult != null) {
				ScoredResult max = maxResult.next();
				ScoredResult[] lastForSegmentArr = lastIndexResultMap.get(max.getIndexName());
				lastForSegmentArr[max.getSegment()] = max;
				results.add(max);
			}
			else {
				isShort = true;
			}
		}
		
		builder.addAllResults(results);
		
		LastResult.Builder newLastResultBuilder = LastResult.newBuilder();
		for (String indexName : lastIndexResultMap.keySet()) {
			ScoredResult[] lastForSegmentArr = lastIndexResultMap.get(indexName);
			int numberOfSegments = usedIndexMap.get(indexName).getNumberOfSegments();
			List<ScoredResult> indexList = new ArrayList<ScoredResult>();
			for (int i = 0; i < numberOfSegments; i++) {
				if (lastForSegmentArr[i] != null) {
					indexList.add(lastForSegmentArr[i]);
				}
			}
			if (!indexList.isEmpty()) {
				LastIndexResult lastIndexResult = LastIndexResult.newBuilder().addAllLastForSegment(indexList).setIndexName(indexName).build();
				newLastResultBuilder.addLastIndexResult(lastIndexResult);
			}
		}
		
		builder.setLastResult(newLastResultBuilder.build());
		
		return builder.build();
	}
	
	public boolean isShort() {
		return isShort;
	}
	
	public void logShort() {
		String msg = "Result set is short. Expected <" + resultsSize + "> only found <" + results.size() + ">\n";
		for (SegmentQueryResult sr : segmentQueryResults) {
			msg += "    ";
			msg += "Segment: " + sr.getSegmentNumber() + "\n";
			msg += "      Current Index: " + sr.getIndex() + "\n";
			msg += "      Score at Current Index: " + (sr.getCurrent() != null ? sr.getCurrent().getScore() : "No Current") + "\n";
			msg += "      Score at Next Index: " + (sr.hasNext() ? sr.previewNext().getScore() : "No Next") + "\n";
			msg += "      Returned Hits: " + sr.getReturnedHits() + "\n";
			msg += "      Total Hits: " + sr.getTotalHits();
			msg += "\n";
		}
		msg += "    If this happens frequently increase requestFactor, minSegmentRequest, or segmentTolerance";
		msg += "    Retrying with full request.";
		log.error(msg);
	}
}
