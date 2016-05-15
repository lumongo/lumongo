package org.lumongo.server.search;

import org.apache.log4j.Logger;
import org.apache.lucene.util.FixedBitSet;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.CountRequest;
import org.lumongo.cluster.message.Lumongo.FacetCount;
import org.lumongo.cluster.message.Lumongo.FacetGroup;
import org.lumongo.cluster.message.Lumongo.FieldConfig;
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
import org.lumongo.server.index.LumongoIndex;

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
	private final int start;
	private final LastResult lastResult;

	private boolean isShort;
	private List<ScoredResult> results;
	private int resultsSize;

	private SortRequest sortRequest;

	private Lumongo.Query query;

	public QueryCombiner(Map<String, LumongoIndex> usedIndexMap, QueryRequest request, List<InternalQueryResponse> responses) {
		this.usedIndexMap = usedIndexMap;
		this.responses = responses;
		this.amount = request.getAmount() + request.getStart();
		this.indexToSegmentResponseMap = new HashMap<>();
		this.segmentResponses = new ArrayList<>();
		this.lastResult = request.getLastResult();
		this.sortRequest = request.getSortRequest();
		this.start = request.getStart();
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
					.map(facet -> new FacetCountResult(facet, facetCounts.get(facet).get())).collect(Collectors.toCollection(TreeSet::new));

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

				if (maxCount > 0 && count > maxCount) {

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

			final HashMap<String, FieldConfig.FieldType> sortTypeMap = new HashMap<>();

			for (FieldSort fieldSort : fieldSortList) {
				String sortField = fieldSort.getSortField();

				for (String indexName : usedIndexMap.keySet()) {
					LumongoIndex index = usedIndexMap.get(indexName);
					FieldConfig.FieldType currentSortType = sortTypeMap.get(sortField);

					FieldConfig.FieldType indexSortType = index.getSortFieldType(sortField);
					if (currentSortType == null) {
						sortTypeMap.put(sortField, indexSortType);
					}
					else {
						if (!currentSortType.equals(indexSortType)) {
							log.error("Sort fields must be defined the same in all indexes searched in a single query");
							String message =
									"Cannot sort on field <" + sortField + ">: found type: <" + currentSortType + "> then type: <" + indexSortType + ">";
							log.error(message);

							throw new Exception(message);
						}
					}
				}
			}

			myCompare = (o1, o2) -> {
				int compare = 0;

				int sortValueIndex = 0;

				Lumongo.SortValues sortValues1 = o1.getSortValues();
				Lumongo.SortValues sortValues2 = o2.getSortValues();
				for (FieldSort fs : fieldSortList) {
					String sortField = fs.getSortField();

					FieldConfig.FieldType sortType = sortTypeMap.get(sortField);

					if (FieldConfig.FieldType.NUMERIC_INT.equals(sortType)) {
						Integer a = null;
						Integer b = null;
						a = sortValues1.getSortValue(sortValueIndex).getIntegerValue();
						b = sortValues2.getSortValue(sortValueIndex).getIntegerValue();

						compare = Comparator.nullsLast(Integer::compareTo).compare(a, b);
					}
					else if (FieldConfig.FieldType.NUMERIC_LONG.equals(sortType) || FieldConfig.FieldType.DATE.equals(sortType)) {
						Long a = null;
						Long b = null;
						a = sortValues1.getSortValue(sortValueIndex).getLongValue();
						b = sortValues2.getSortValue(sortValueIndex).getLongValue();

						compare = Comparator.nullsLast(Long::compareTo).compare(a, b);
					}
					else if (FieldConfig.FieldType.NUMERIC_FLOAT.equals(sortType)) {
						Float a = null;
						Float b = null;
						a = sortValues1.getSortValue(sortValueIndex).getFloatValue();
						b = sortValues2.getSortValue(sortValueIndex).getFloatValue();

						compare = Comparator.nullsLast(Float::compareTo).compare(a, b);
					}
					else if (FieldConfig.FieldType.NUMERIC_DOUBLE.equals(sortType)) {
						Double a = null;
						Double b = null;
						a = sortValues1.getSortValue(sortValueIndex).getDoubleValue();
						b = sortValues2.getSortValue(sortValueIndex).getDoubleValue();

						compare = Comparator.nullsLast(Double::compareTo).compare(a, b);
					}
					else {
						String a = null;
						String b = null;
						a = sortValues1.getSortValue(sortValueIndex).getStringValue();
						b = sortValues2.getSortValue(sortValueIndex).getStringValue();

						compare = Comparator.nullsLast(String::compareTo).compare(a, b);
					}

					if (FieldSort.Direction.DESCENDING.equals(fs.getDirection())) {
						compare *= -1;
					}

					if (compare != 0) {
						return compare;
					}

					sortValueIndex++;

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
								msg += "    Last for index from segment <" + lastForIndex.getSegment() + "> has sort values <" + lastForIndex.getSortValues()
										+ ">\n";
								msg += "    Next for segment <" + next.getSegment() + ">  has sort values <" + next.getSortValues() + ">\n";
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


		int i = 0;
		for (ScoredResult scoredResult : results) {
			if (i >= start) {
				builder.addResults(scoredResult);
			}
			i++;
		}


		LastResult.Builder newLastResultBuilder = LastResult.newBuilder();
		for (String indexName : lastIndexResultMap.keySet()) {
			ScoredResult[] lastForSegmentArr = lastIndexResultMap.get(indexName);
			int numberOfSegments = usedIndexMap.get(indexName).getNumberOfSegments();
			List<ScoredResult> indexList = new ArrayList<>();
			for (int seg = 0; seg < numberOfSegments; seg++) {
				if (lastForSegmentArr[seg] != null) {
					ScoredResult.Builder minimalSR = ScoredResult.newBuilder(lastForSegmentArr[seg]);
					minimalSR = minimalSR.clearUniqueId().clearIndexName().clearResultIndex().clearTimestamp().clearResultDocument();
					indexList.add(minimalSR.build());
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
