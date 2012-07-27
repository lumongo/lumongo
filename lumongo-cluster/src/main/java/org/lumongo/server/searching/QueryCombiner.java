package org.lumongo.server.searching;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FieldSort;
import org.lumongo.cluster.message.Lumongo.IndexSegmentResponse;
import org.lumongo.cluster.message.Lumongo.InternalQueryResponse;
import org.lumongo.cluster.message.Lumongo.LastIndexResult;
import org.lumongo.cluster.message.Lumongo.LastResult;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.cluster.message.Lumongo.SegmentResponse;
import org.lumongo.cluster.message.Lumongo.SortRequest;
import org.lumongo.server.indexing.Index;

public class QueryCombiner {
	
    private final static Comparator<ScoredResult> scoreCompare = new ScoreCompare();
    
	private final static Logger log = Logger.getLogger(QueryCombiner.class);
	
	private final Map<String, Index> usedIndexMap;
	private final List<InternalQueryResponse> responses;
	
	private final Map<String, Map<Integer, SegmentResponse>> indexToSegmentResponseMap;
	private final List<SegmentResponse> segmentResponses;
	
	private final int amount;
	private final LastResult lastResult;
	
	private boolean isShort;
	private List<ScoredResult> results;
	private int resultsSize;

    private SortRequest sortRequest;

    private String query;
	
	public QueryCombiner(Map<String, Index> usedIndexMap, QueryRequest request, List<InternalQueryResponse> responses) {
		this.usedIndexMap = usedIndexMap;
		this.responses = responses;
		this.amount = request.getAmount();
		this.indexToSegmentResponseMap = new HashMap<String, Map<Integer, SegmentResponse>>();
		this.segmentResponses = new ArrayList<SegmentResponse>();
		this.lastResult = request.getLastResult();
		this.sortRequest = request.getSortRequest();
		this.query = request.getQuery();
		this.isShort = false;
		this.results = Collections.emptyList();
		this.resultsSize = 0;
	}
	


    public void validate() throws Exception {
		for (InternalQueryResponse iqr : responses) {
			
			for (IndexSegmentResponse isr : iqr.getIndexSegmentResponseList()) {
				String indexName = isr.getIndexName();
				if (!indexToSegmentResponseMap.containsKey(indexName)) {
					indexToSegmentResponseMap.put(indexName, new HashMap<Integer, SegmentResponse>());
				}
				
				for (SegmentResponse sr : isr.getSegmentReponseList()) {
					int segmentNumber = sr.getSegmentNumber();
					
					Map<Integer, SegmentResponse> segmentResponseMap = indexToSegmentResponseMap.get(indexName);
					
					if (segmentResponseMap.containsKey(segmentNumber)) {
						throw new Exception("Segment <" + segmentNumber + "> is repeated for <" + indexName + ">");
					}
					else {
						segmentResponseMap.put(segmentNumber, sr);
						segmentResponses.add(sr);
					}
				}
				
			}
			
		}
		
		for (String indexName : usedIndexMap.keySet()) {
			int numberOfSegments = usedIndexMap.get(indexName).getNumberOfSegments();
			Map<Integer, SegmentResponse> segmentResponseMap = indexToSegmentResponseMap.get(indexName);
			
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
	    
	    boolean sorting = (sortRequest != null && !sortRequest.getFieldSortList().isEmpty());

		long totalHits = 0;
		long returnedHits = 0;
		for (SegmentResponse sr : segmentResponses) {
			totalHits += sr.getTotalHits();
			returnedHits += sr.getScoredResultList().size();
		}
		
		QueryResponse.Builder builder = QueryResponse.newBuilder();
		builder.setTotalHits(totalHits);
		
		resultsSize = Math.min(amount, (int) returnedHits);
		
		results = Collections.emptyList();
		
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
		for (SegmentResponse sr : segmentResponses) {
			for (FacetCount fc : sr.getFacetCountList()) {
				String facet = fc.getFacet();
				if (!totalFacetCounts.containsKey(facet)) {
					totalFacetCounts.put(facet, new AtomicLong());
				}
				totalFacetCounts.get(facet).addAndGet(fc.getCount());
			}
		}
		
		SortedSet<FacetCountResult> sortedFacetResuls = new TreeSet<FacetCountResult>();
		for (String facet : totalFacetCounts.keySet()) {
			sortedFacetResuls.add(new FacetCountResult(facet, totalFacetCounts.get(facet).get()));
		}
		
		for (FacetCountResult facet : sortedFacetResuls) {
			builder.addFacetCount(FacetCount.newBuilder().setFacet(facet.getFacet()).setCount(facet.getCount()));
		}
	
		List<ScoredResult> mergedResults = new ArrayList<ScoredResult>((int)returnedHits);
		for (SegmentResponse sr : segmentResponses) {
		    mergedResults.addAll(sr.getScoredResultList());
		}
		
		Comparator<ScoredResult> myCompare = scoreCompare;
		
		if (sorting) {
		    final List<FieldSort> fieldSortList = sortRequest.getFieldSortList();
		    Comparator<ScoredResult> sortCompare = new Comparator<ScoredResult>() {

                @Override
                public int compare(ScoredResult o1, ScoredResult o2) {
                    int compare = 0;
                    
                    int i = 0;
                    for (FieldSort fs : fieldSortList) {
                        String a = o1.getSortTermsList().get(i);
                        String b = o2.getSortTermsList().get(i);
                        
                        if (a == null) {
                            a = ""; //TODO think about whether null and empty should be same
                            //what does lucene do?
                        }
                        if (b == null) {
                            b = ""; //TODO think about whether null and empty should be same
                            //what does lucene do?
                        }
                        
                        compare = a.compareTo(b);
                        
                        if (FieldSort.Direction.DESCENDING.equals(fs.getDirection())) {
                            compare *= -1;
                        }
                        
                        if (compare != 0) {
                            return compare;
                        }
                        
                        i++;
                    }
                    
                    
                    return compare;
                }
		        
		    };
		    myCompare = sortCompare;
		}

		
		
		if (!mergedResults.isEmpty()) {
		    Collections.sort(mergedResults, myCompare);
		    results = mergedResults.subList(0, resultsSize);
		    
		    for (ScoredResult sr : results) {
		        ScoredResult[] lastForSegmentArr = lastIndexResultMap.get(sr.getIndexName());
		        lastForSegmentArr[sr.getSegment()] = sr;
		    }
		    
		    outside:
		    for (String indexName : usedIndexMap.keySet()) {
		        ScoredResult[] lastForSegmentArr = lastIndexResultMap.get(indexName);
		        ScoredResult lastForIndex = null;
		        for (ScoredResult sr : lastForSegmentArr) {
		            if (sr != null) {
		                if (lastForIndex == null) {
		                    lastForIndex = sr;
		                }
		                else {
		                    if (myCompare.compare(sr, lastForIndex) < 0) {
		                        lastForIndex = sr;
		                    }
		                }
		            }
		        }
		        
		        double segmentTolerance = usedIndexMap.get(indexName).getSegmentTolerance();
		        		        
	            int numberOfSegments = usedIndexMap.get(indexName).getNumberOfSegments();
	            Map<Integer, SegmentResponse> segmentResponseMap = indexToSegmentResponseMap.get(indexName);
	            for (int segmentNumber = 0; segmentNumber < numberOfSegments; segmentNumber++) {
	                SegmentResponse sr = segmentResponseMap.get(segmentNumber);
	                if (sr.hasNext()) {
	                    ScoredResult next = sr.getNext();
	                    if (myCompare.compare(lastForIndex, next) < 0) {
	                        	                        
                            if (sorting) {
                                String msg = "Result set did not return the most relevant sorted documents for index <" + indexName + ">\n";
                                msg += "    Last for index from segment <" + lastForIndex.getSegment() + "> has sort values <" +lastForIndex.getSortTermsList() + ">\n";
                                msg += "    Next for segment <" + next.getSegment() + ">  has sort values <" +next.getSortTermsList() + ">\n"; 
                                msg += "    Next for segment <" + next.getSegment() + ">  has sort values <" +next.getSortTermsList() + ">\n";
                                msg += "    Last for segments: \n";
                                msg += "      " + Arrays.toString(lastForSegmentArr) + "\n";
                                msg += "    Results: \n";
                                msg += "      " + results + "\n";
                                msg += "    If this happens frequently increase requestFactor or minSegmentRequest\n";
                                msg += "    Retrying with full request.\n";
                                log.error(msg);
                                
                                isShort = true;
                                break outside;
	                        }
                            
                            double diff = (Math.abs(lastForIndex.getScore() - next.getScore()));
	                        if (diff > segmentTolerance) {
	                            String msg = "Result set did not return the most relevant documents for index <" + indexName + "> with segment tolerance <" + segmentTolerance + ">\n";
	                            msg += "    Query <" + query + ">\n";
	                            msg += "    Last for index from segment <" + lastForIndex.getSegment() + "> has score <" +lastForIndex.getScore() + ">\n";
	                            msg += "    Next for segment <" + next.getSegment() + "> has score <" +next.getScore() + ">\n";	                            
	                            msg += "    If this happens frequently increase requestFactor, minSegmentRequest, or segmentTolerance\n";
	                            msg += "    Retrying with full request.\n";
	                            log.error(msg);
	                            
	                            isShort = true;
	                            break outside;
	                        }
                        }
	                }
	            }
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

}
