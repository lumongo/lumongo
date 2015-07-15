package org.lumongo.server.searching;

import org.apache.log4j.Logger;
import org.apache.lucene.util.FixedBitSet;
import org.lumongo.cluster.message.Lumongo.CountRequest;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FacetGroup;
import org.lumongo.cluster.message.Lumongo.FieldSort;
import org.lumongo.cluster.message.Lumongo.IndexSegmentResponse;
import org.lumongo.cluster.message.Lumongo.InternalQueryResponse;
import org.lumongo.cluster.message.Lumongo.LMAnalyzer;
import org.lumongo.cluster.message.Lumongo.LastIndexResult;
import org.lumongo.cluster.message.Lumongo.LastResult;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.ScoredResult;
import org.lumongo.cluster.message.Lumongo.SegmentResponse;
import org.lumongo.cluster.message.Lumongo.SortRequest;
import org.lumongo.server.indexing.LumongoIndex;

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
import java.util.stream.Collectors;

public class QueryCombiner {
	
	private final static Comparator<ScoredResult> scoreCompare = new ScoreCompare();
	
	private final static Logger log = Logger.getLogger(QueryCombiner.class);
	
	private final Map<String, LumongoIndex> usedIndexMap;
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
	
	public QueryCombiner(Map<String, LumongoIndex> usedIndexMap, QueryRequest request, List<InternalQueryResponse> responses) {
		this.usedIndexMap = usedIndexMap;
		this.responses = responses;
		this.amount = request.getAmount();
		this.indexToSegmentResponseMap = new HashMap<>();
		this.segmentResponses = new ArrayList<>();
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
					indexToSegmentResponseMap.put(indexName, new HashMap<>());
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
	
	public QueryResponse getQueryResponse() throws Exception {
		
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
		
		Map<String, ScoredResult[]> lastIndexResultMap = new HashMap<>();
		
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
		
		Map<CountRequest, Map<String, AtomicLong>> facetCountsMap = new HashMap<>();
		Map<CountRequest, Map<String, FixedBitSet>> segmentsReturnedMap = new HashMap<>();
		Map<CountRequest, FixedBitSet> fullResultsMap = new HashMap<>();
		Map<CountRequest, long[]> minForSegmentMap = new HashMap<>();

		int segIndex = 0;

		for (SegmentResponse sr : segmentResponses) {

			for (FacetGroup fg : sr.getFacetGroupList()) {
				
				CountRequest countRequest = fg.getCountRequest();
				Map<String, AtomicLong> facetCounts = facetCountsMap.get(countRequest);
				Map<String, FixedBitSet> segmentsReturned = segmentsReturnedMap.get(countRequest);
				FixedBitSet fullResults = fullResultsMap.get(countRequest);
				long[] minForSegment = minForSegmentMap.get(countRequest);

				if (facetCounts == null) {
					facetCounts = new HashMap<>();
					facetCountsMap.put(countRequest, facetCounts);

					segmentsReturned = new HashMap<>();
					segmentsReturnedMap.put(countRequest, segmentsReturned);

					fullResults = new FixedBitSet(segmentResponses.size());
					fullResultsMap.put(countRequest, fullResults);

					minForSegment = new long[segmentResponses.size()];
					minForSegmentMap.put(countRequest, minForSegment);
				}

				for (FacetCount fc : fg.getFacetCountList()) {
					String facet = fc.getFacet();
					AtomicLong facetSum = facetCounts.get(facet);
					FixedBitSet segmentSet = segmentsReturned.get(facet);
					
					if (facetSum == null) {
						facetSum = new AtomicLong();
						facetCounts.put(facet, facetSum);
						segmentSet = new FixedBitSet(segmentResponses.size());
						segmentsReturned.put(facet, segmentSet);
					}
					long count = fc.getCount();
					facetSum.addAndGet(count);
					segmentSet.set(segIndex);

					minForSegment[segIndex] = count;
				}

				int segmentFacets = countRequest.getSegmentFacets();
				int facetCountCount = fg.getFacetCountCount();
				if (facetCountCount < segmentFacets || (segmentFacets == 0)) {
					fullResults.set(segIndex);
					minForSegment[segIndex] = 0;
					System.out.println(segIndex + ":" + facetCountCount + ":" + segmentFacets);
				}
			}

			segIndex++;
		}

		for (CountRequest countRequest : facetCountsMap.keySet()) {

			FacetGroup.Builder fg = FacetGroup.newBuilder();
			fg.setCountRequest(countRequest);
			Map<String, AtomicLong> facetCounts = facetCountsMap.get(countRequest);
			Map<String, FixedBitSet> segmentsReturned = segmentsReturnedMap.get(countRequest);
			FixedBitSet fullResults = fullResultsMap.get(countRequest);
			long[] minForSegment = minForSegmentMap.get(countRequest);

			int numberOfSegments = segmentResponses.size();
			long maxValuePossibleMissing = 0;
			for (int i = 0; i < numberOfSegments; i++) {
				maxValuePossibleMissing += minForSegment[i];
			}

			boolean computeError = countRequest.getSegmentFacets() != 0 && countRequest.getComputeError();
			boolean computePossibleMissing = countRequest.getSegmentFacets() != 0 && countRequest.getComputePossibleMissed() && (maxValuePossibleMissing != 0);

			SortedSet<FacetCountResult> sortedFacetResults = facetCounts.keySet().stream()
							.map(facet -> new FacetCountResult(facet, facetCounts.get(facet).get()))
							.collect(Collectors.toCollection(TreeSet::new));

			Integer maxCount = countRequest.getMaxFacets();

			long minCountReturned = 0;

			int count = 0;
			for (FacetCountResult facet : sortedFacetResults) {

				FixedBitSet segCount = segmentsReturned.get(facet.getFacet());
				segCount.or(fullResults);

				FacetCount.Builder facetCountBuilder = FacetCount.newBuilder().setFacet(facet.getFacet()).setCount(facet.getCount());

				long maxWithError = 0;
				if (computeError) {
					long maxError = 0;
					if (segCount.cardinality() < numberOfSegments) {
						for (int i = 0; i < numberOfSegments; i++) {
							if (!segCount.get(i)) {
								maxError += minForSegment[i];
							}
						}
					}
					facetCountBuilder.setMaxError(maxError);
					maxWithError = maxError + facet.getCount();
				}

				count++;

				if (maxCount > 0 && count >= maxCount) {

					if (computePossibleMissing) {
						if (maxWithError > maxValuePossibleMissing) {
							maxValuePossibleMissing = maxWithError;
						}
					}
					else {
						break;
					}
				}
				else {
					fg.addFacetCount(facetCountBuilder);
					minCountReturned = facet.getCount();
				}
			}

			if (!sortedFacetResults.isEmpty()) {
				if (maxValuePossibleMissing > minCountReturned) {
					fg.setPossibleMissing(true);
					fg.setMaxValuePossibleMissing(maxValuePossibleMissing);
				}
			}

			builder.addFacetGroup(fg);
		}
		
		List<ScoredResult> mergedResults = new ArrayList<>((int) returnedHits);
		for (SegmentResponse sr : segmentResponses) {
			mergedResults.addAll(sr.getScoredResultList());
		}
		
		Comparator<ScoredResult> myCompare = scoreCompare;
		
		if (sorting) {
			final List<FieldSort> fieldSortList = sortRequest.getFieldSortList();
			
			final HashMap<String, LMAnalyzer> analyzerMap = new HashMap<>();
			
			for (FieldSort fieldSort : fieldSortList) {
				String sortField = fieldSort.getSortField();
				
				LMAnalyzer lmAnalyzer = null;
				for (String indexName : usedIndexMap.keySet()) {
					LumongoIndex index = usedIndexMap.get(indexName);
					if (lmAnalyzer == null) {
						lmAnalyzer = index.getLMAnalyzer(sortField);
						analyzerMap.put(sortField, lmAnalyzer);
					}
					else {
						if (!lmAnalyzer.equals(index.getLMAnalyzer(sortField))) {
							log.error("Sort fields must be defined the same in all indexes searched in a single query");
							String message = "Cannot sort on field <" + sortField + ">: found type: <" + lmAnalyzer + "> then type: <"
											+ index.getLMAnalyzer(sortField) + ">";
							log.error(message);
							
							throw new Exception(message);
						}
					}
				}
			}
			
			myCompare = (o1, o2) -> {
				int compare = 0;

				int stringIndex = 0;
				int intIndex = 0;
				int longIndex = 0;
				int floatIndex = 0;
				int doubleIndex = 0;
				int dateIndex = 0;

				for (FieldSort fs : fieldSortList) {
					String sortField = fs.getSortField();
					LMAnalyzer lmAnalyzer = analyzerMap.get(sortField);

					if (LMAnalyzer.NUMERIC_INT.equals(lmAnalyzer)) {
						int a = o1.getSortIntegerList().get(intIndex);
						int b = o2.getSortIntegerList().get(intIndex);
						compare = Integer.compare(a, b);
						intIndex++;
					}
					else if (LMAnalyzer.NUMERIC_LONG.equals(lmAnalyzer)) {
						long a = o1.getSortLongList().get(longIndex);
						long b = o2.getSortLongList().get(longIndex);
						compare = Long.compare(a, b);
						longIndex++;
					}
					else if (LMAnalyzer.NUMERIC_FLOAT.equals(lmAnalyzer)) {
						float a = o1.getSortFloatList().get(floatIndex);
						float b = o2.getSortFloatList().get(floatIndex);
						compare = Float.compare(a, b);
						floatIndex++;
					}
					else if (LMAnalyzer.NUMERIC_DOUBLE.equals(lmAnalyzer)) {
						double a = o1.getSortFloatList().get(doubleIndex);
						double b = o2.getSortFloatList().get(doubleIndex);
						compare = Double.compare(a, b);
						doubleIndex++;
					}
					else if (LMAnalyzer.DATE.equals(lmAnalyzer)) {
						long a = o1.getSortDateList().get(dateIndex);
						long b = o2.getSortDateList().get(dateIndex);
						compare = Long.compare(a, b);
						dateIndex++;
					}
					else if (LMAnalyzer.KEYWORD.equals(lmAnalyzer) || LMAnalyzer.LC_KEYWORD.equals(lmAnalyzer)) {
						String a = o1.getSortTermList().get(stringIndex);
						String b = o2.getSortTermList().get(stringIndex);
						compare = a.compareTo(b);
						stringIndex++;
					}
					else {
						throw new RuntimeException("Unsupported analyzer <" + lmAnalyzer + "> for sort field <" + sortField + ">");
					}

					if (FieldSort.Direction.DESCENDING.equals(fs.getDirection())) {
						compare *= -1;
					}

					if (compare != 0) {
						return compare;
					}

				}

				return compare;
			};
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
							if (myCompare.compare(sr, lastForIndex) > 0) {
								lastForIndex = sr;
							}
						}
					}
				}
				
				if (lastForIndex == null) {
					//this happen when amount from index is zero
					continue;
				}
				
				double segmentTolerance = usedIndexMap.get(indexName).getSegmentTolerance();
				
				int numberOfSegments = usedIndexMap.get(indexName).getNumberOfSegments();
				Map<Integer, SegmentResponse> segmentResponseMap = indexToSegmentResponseMap.get(indexName);
				for (int segmentNumber = 0; segmentNumber < numberOfSegments; segmentNumber++) {
					SegmentResponse sr = segmentResponseMap.get(segmentNumber);
					if (sr.hasNext()) {
						ScoredResult next = sr.getNext();
						int compare = myCompare.compare(lastForIndex, next);
						if (compare > 0) {
							
							if (sorting) {
								String msg = "Result set did not return the most relevant sorted documents for index <" + indexName + ">\n";
								msg += "    Last for index from segment <" + lastForIndex.getSegment() + "> has sort values <" + lastForIndex.getSortTermList()
												+ ">\n";
								msg += "    Next for segment <" + next.getSegment() + ">  has sort values <" + next.getSortTermList() + ">\n";
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
								String msg = "Result set did not return the most relevant documents for index <" + indexName + "> with segment tolerance <"
												+ segmentTolerance + ">\n";
								msg += "    Query <" + query + ">\n";
								msg += "    Last for index from segment <" + lastForIndex.getSegment() + "> has score <" + lastForIndex.getScore() + ">\n";
								msg += "    Next for segment <" + next.getSegment() + "> has score <" + next.getScore() + ">\n";
								msg += "    Last for segments: \n";
								msg += "      " + Arrays.toString(lastForSegmentArr) + "\n";
								msg += "    Results: \n";
								msg += "      " + results + "\n";
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
			List<ScoredResult> indexList = new ArrayList<>();
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
