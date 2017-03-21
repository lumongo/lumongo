package org.lumongo.client.command;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.QueryResult;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.AnalysisRequest;
import org.lumongo.cluster.message.Lumongo.CountRequest;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.FacetRequest;
import org.lumongo.cluster.message.Lumongo.FieldSort;
import org.lumongo.cluster.message.Lumongo.FieldSort.Direction;
import org.lumongo.cluster.message.Lumongo.HighlightRequest;
import org.lumongo.cluster.message.Lumongo.LMFacet;
import org.lumongo.cluster.message.Lumongo.LastResult;
import org.lumongo.cluster.message.Lumongo.Query.Operator;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.SortRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Runs a query on one of more LuMongo indexes.
 * @author mdavis
 *
 */
public class Query extends SimpleCommand<QueryRequest, QueryResult> {

	private String query;
	private int amount;
	private int start;
	private Collection<String> indexes;
	private LastResult lastResult;
	private List<CountRequest> countRequests = Collections.emptyList();
	private List<LMFacet> drillDowns = Collections.emptyList();
	private List<FieldSort> fieldSorts = Collections.emptyList();
	private Set<String> queryFields = Collections.emptySet();
	private List<Lumongo.Query> filterQueries = Collections.emptyList();
	private List<HighlightRequest> highlightRequests = Collections.emptyList();
	private List<AnalysisRequest> analysisRequests = Collections.emptyList();
	private Integer minimumNumberShouldMatch;
	private Operator defaultOperator;
	private Lumongo.FetchType resultFetchType;
	private Set<String> documentFields = Collections.emptySet();
	private Set<String> documentMaskedFields = Collections.emptySet();
	private List<Lumongo.FieldSimilarity> fieldSimilarities = Collections.emptyList();
	private List<Lumongo.CosineSimRequest> cosineSimRequests = Collections.emptyList();
	private Boolean dismax;
	private Float dismaxTie;
	private Boolean dontCache;
	private Boolean debug;

	public Query(String index, String query, int amount) {
		this(new String[] { index }, query, amount);
	}

	public Query(String[] indexes, String query, int amount) {
		this(new ArrayList<>(Arrays.asList(indexes)), query, amount);
	}

	public Query(Collection<String> indexes, String query, int amount) {
		this.indexes = indexes;
		this.query = query;
		this.amount = amount;
	}

	public Float getDismaxTie() {
		return dismaxTie;
	}

	public Query setDismaxTie(Float dismaxTie) {
		this.dismaxTie = dismaxTie;
		return this;
	}

	public Boolean getDismax() {
		return dismax;
	}

	public Query setDismax(Boolean dismax) {
		this.dismax = dismax;
		return this;
	}

	public String getQuery() {
		return query;
	}

	public Query setQuery(String query) {
		this.query = query;
		return this;
	}

	public int getAmount() {
		return amount;
	}

	public Query setAmount(int amount) {
		this.amount = amount;
		return this;
	}

	public int getStart() {
		return start;
	}

	public Query setStart(int start) {
		this.start = start;
		return this;
	}

	public Integer getMinimumNumberShouldMatch() {
		return minimumNumberShouldMatch;
	}

	public void setMinimumNumberShouldMatch(Integer minimumNumberShouldMatch) {
		this.minimumNumberShouldMatch = minimumNumberShouldMatch;
	}

	public Collection<String> getIndexes() {
		return indexes;
	}

	public Query setIndexes(Collection<String> indexes) {
		this.indexes = indexes;
		return this;
	}

	public Query setLastResult(QueryResult lastQueryResult) {
		this.lastResult = lastQueryResult.getLastResult();
		return this;
	}

	public LastResult getLastResult() {
		return lastResult;
	}

	public Query setLastResult(LastResult lastResult) {
		this.lastResult = lastResult;
		return this;
	}

	public Query addDrillDown(String label, String path) {
		if (drillDowns.isEmpty()) {
			this.drillDowns = new ArrayList<>();
		}

		drillDowns.add(LMFacet.newBuilder().setLabel(label).setPath(path).build());
		return this;
	}

	public List<LMFacet> getDrillDowns() {
		return drillDowns;
	}

	public Set<String> getQueryFields() {
		return queryFields;
	}

	public Query setQueryFields(String... queryFields) {
		this.queryFields = new HashSet<>(Arrays.asList(queryFields));
		return this;

	}

	public Query setQueryFields(Collection<String> queryFields) {
		this.queryFields = new HashSet<String>(queryFields);
		return this;
	}

	public Query addFieldSimilarity(String field, Lumongo.AnalyzerSettings.Similarity similarity) {

		Lumongo.FieldSimilarity fieldSimilarity = Lumongo.FieldSimilarity.newBuilder().setField(field).setSimilarity(similarity).build();

		return addFieldSimilarity(fieldSimilarity);
	}

	private Query addFieldSimilarity(Lumongo.FieldSimilarity fieldSimilarity) {
		if (fieldSimilarities.isEmpty()) {
			fieldSimilarities = new ArrayList<>();
		}

		fieldSimilarities.add(fieldSimilarity);

		return this;
	}

	public Query addCosineSim(String field, double[] vector, double similarity) {

		Lumongo.CosineSimRequest.Builder builder = Lumongo.CosineSimRequest.newBuilder().setField(field).setSimilarity(similarity);
		for (int i = 0; i < vector.length; i++) {
			builder.addVector(vector[i]);
		}
		addCosineSim(builder.build());
		return this;
	}

	private Query addCosineSim(Lumongo.CosineSimRequest cosineSimRequest) {
		if (cosineSimRequests.isEmpty()) {
			cosineSimRequests = new ArrayList<>();
		}

		cosineSimRequests.add(cosineSimRequest);

		return this;
	}

	public Query addQueryField(String queryField) {
		if (queryFields.isEmpty()) {
			this.queryFields = new HashSet<>();
		}

		queryFields.add(queryField);
		return this;
	}

	public Query addQueryField(String... queryFields) {
		if (this.queryFields.isEmpty()) {
			this.queryFields = new HashSet<>();
		}

		for (String queryField : queryFields) {
			this.queryFields.add(queryField);
		}
		return this;
	}

	public List<Lumongo.Query> getFilterQueries() {
		return filterQueries;
	}

	public void setFilterQueries(List<Lumongo.Query> filterQueries) {
		this.filterQueries = filterQueries;
	}

	public Query addFilterQuery(String query) {
		return addFilterQuery(query, null, null, null);
	}

	public Query addFilterQuery(String query, Collection<String> queryFields) {
		return addFilterQuery(query, queryFields, null, null);
	}

	public Query addFilterQuery(String query, Collection<String> queryFields, Operator defaultOperator) {
		return addFilterQuery(query, queryFields, defaultOperator, null);
	}

	public Query addFilterQuery(String query, Collection<String> queryFields, Integer minimumNumberShouldMatch) {
		return addFilterQuery(query, queryFields, null, minimumNumberShouldMatch);
	}

	public Query addFilterQuery(String query, Collection<String> queryFields, Operator defaultOperator, Integer minimumNumberShouldMatch) {
		if (filterQueries.isEmpty()) {
			this.filterQueries = new ArrayList<>();
		}

		Lumongo.Query.Builder builder = Lumongo.Query.newBuilder();
		if (query != null && !query.isEmpty()) {
			builder.setQ(query);
		}
		if (minimumNumberShouldMatch != null) {
			builder.setMm(minimumNumberShouldMatch);
		}
		if (defaultOperator != null) {
			builder.setDefaultOp(defaultOperator);
		}
		if (queryFields != null && !queryFields.isEmpty()) {
			builder.addAllQf(queryFields);
		}
		filterQueries.add(builder.build());
		return this;
	}

	public Query addHighlight(String field) {
		return addHighlight(field, null, null, null);
	}

	public Query addHighlight(String field, Integer numberOfFragments) {
		return addHighlight(field, null, null, numberOfFragments);
	}

	public Query addHighlight(String field, String preTag, String postTag, Integer numberOfFragments) {
		if (highlightRequests.isEmpty()) {
			this.highlightRequests = new ArrayList<>();
		}

		HighlightRequest.Builder highlight = HighlightRequest.newBuilder();
		if (field != null && !field.isEmpty()) {
			highlight.setField(field);
		}
		if (preTag != null) {
			highlight.setPreTag(preTag);
		}
		if (postTag != null) {
			highlight.setPostTag(postTag);
		}
		if (numberOfFragments != null) {
			highlight.setNumberOfFragments(numberOfFragments);
		}

		highlightRequests.add(highlight.build());
		return this;
	}

	public Query addSummaryAnalysis(String field, int topN) {
		return addAnalysis(field, false, false, true, topN);
	}

	public Query addAnalysis(String field, Boolean tokens, Boolean docTerms, Boolean summaryTerms, Integer topN) {

		AnalysisRequest.Builder analysisRequest = AnalysisRequest.newBuilder();
		if (field != null && !field.isEmpty()) {
			analysisRequest.setField(field);
		}
		if (tokens != null) {
			analysisRequest.setTokens(tokens);
		}
		if (docTerms != null) {
			analysisRequest.setDocTerms(docTerms);
		}
		if (summaryTerms != null) {
			analysisRequest.setSummaryTerms(summaryTerms);
		}
		if (topN != null) {
			analysisRequest.setTopN(topN);
		}

		return addAnalysis(analysisRequest);
	}

	public Query addAnalysis(AnalysisRequest.Builder analysisRequest) {
		if (analysisRequests.isEmpty()) {
			this.analysisRequests = new ArrayList<>();
		}
		analysisRequests.add(analysisRequest.build());
		return this;
	}

	public Query addCountRequest(String label) {
		return (addCountRequest(label, null));
	}

	public Query addCountRequest(String label, Integer maxFacets) {
		return (addCountRequest(label, maxFacets, null));
	}

	public Query addCountRequest(String label, Integer maxFacets, Integer segmentFacets) {

		CountRequest.Builder countRequest = CountRequest.newBuilder().setFacetField(LMFacet.newBuilder().setLabel(label).build());
		if (maxFacets != null) {
			countRequest.setMaxFacets(maxFacets);
		}
		if (segmentFacets != null) {
			countRequest.setSegmentFacets(segmentFacets);
		}
		if (countRequests.isEmpty()) {
			this.countRequests = new ArrayList<>();
		}
		countRequests.add(countRequest.build());
		return this;
	}

	public List<CountRequest> getCountRequests() {
		return countRequests;
	}

	public Query addFieldSort(String sort) {
		if (fieldSorts.isEmpty()) {
			this.fieldSorts = new ArrayList<>();
		}
		fieldSorts.add(FieldSort.newBuilder().setSortField(sort).setDirection(Direction.ASCENDING).build());
		return this;
	}

	public Query addFieldSort(String sort, Direction direction) {
		if (fieldSorts.isEmpty()) {
			this.fieldSorts = new ArrayList<>();
		}
		fieldSorts.add(FieldSort.newBuilder().setSortField(sort).setDirection(direction).build());
		return this;
	}

	public Operator getDefaultOperator() {
		return defaultOperator;
	}

	public Query setDefaultOperator(Operator defaultOperator) {
		this.defaultOperator = defaultOperator;
		return this;
	}

	public Lumongo.FetchType getResultFetchType() {
		return resultFetchType;
	}

	public Query setResultFetchType(Lumongo.FetchType resultFetchType) {
		this.resultFetchType = resultFetchType;
		return this;
	}

	public Set<String> getDocumentMaskedFields() {
		return documentMaskedFields;
	}

	public Query addDocumentMaskedField(String documentMaskedField) {
		if (documentMaskedFields.isEmpty()) {
			documentMaskedFields = new LinkedHashSet<>();
		}

		documentMaskedFields.add(documentMaskedField);
		return this;
	}

	public Set<String> getDocumentFields() {
		return documentFields;
	}

	public Query addDocumentField(String documentField) {
		if (documentFields.isEmpty()) {
			this.documentFields = new LinkedHashSet<>();
		}
		documentFields.add(documentField);
		return this;
	}

	public Boolean getDontCache() {
		return dontCache;
	}

	public void setDontCache(Boolean dontCache) {
		this.dontCache = dontCache;
	}

	public Boolean getDebug() {
		return debug;
	}

	public void setDebug(Boolean debug) {
		this.debug = debug;
	}

	@Override
	public QueryRequest getRequest() {
		QueryRequest.Builder requestBuilder = QueryRequest.newBuilder();
		requestBuilder.setAmount(amount);
		requestBuilder.setStart(start);
		requestBuilder.setDebug(debug);

		Lumongo.Query.Builder queryBuilder = Lumongo.Query.newBuilder();
		if (query != null) {
			queryBuilder.setQ(query);
		}
		if (minimumNumberShouldMatch != null) {
			queryBuilder.setMm(minimumNumberShouldMatch);
		}
		if (!queryFields.isEmpty()) {
			queryBuilder.addAllQf(queryFields);
		}
		if (defaultOperator != null) {
			queryBuilder.setDefaultOp(defaultOperator);
		}

		if (dismax != null) {
			queryBuilder.setDismax(dismax);
			if (dismaxTie != null) {
				queryBuilder.setDismaxTie(dismaxTie);
			}
		}

		requestBuilder.setQuery(queryBuilder);

		if (lastResult != null) {
			requestBuilder.setLastResult(lastResult);
		}

		if (!fieldSimilarities.isEmpty()) {
			requestBuilder.addAllFieldSimilarity(fieldSimilarities);
		}

		for (String index : indexes) {
			requestBuilder.addIndex(index);
		}

		if (!drillDowns.isEmpty() || !countRequests.isEmpty()) {
			FacetRequest.Builder facetRequestBuilder = FacetRequest.newBuilder();

			facetRequestBuilder.addAllDrillDown(drillDowns);

			facetRequestBuilder.addAllCountRequest(countRequests);

			requestBuilder.setFacetRequest(facetRequestBuilder.build());

		}

		if (!filterQueries.isEmpty()) {
			requestBuilder.addAllFilterQuery(filterQueries);
		}

		if (!highlightRequests.isEmpty()) {
			requestBuilder.addAllHighlightRequest(highlightRequests);
		}

		if (!analysisRequests.isEmpty()) {
			requestBuilder.addAllAnalysisRequest(analysisRequests);
		}

		if (resultFetchType != null) {
			requestBuilder.setResultFetchType(resultFetchType);
		}

		if (!cosineSimRequests.isEmpty()) {
			requestBuilder.addAllCosineSimRequest(cosineSimRequests);
		}

		if (dontCache != null) {
			requestBuilder.setDontCache(dontCache);
		}

		requestBuilder.addAllDocumentFields(documentFields);
		requestBuilder.addAllDocumentMaskedFields(documentMaskedFields);

		SortRequest.Builder sortRequestBuilder = SortRequest.newBuilder();
		sortRequestBuilder.addAllFieldSort(fieldSorts);
		requestBuilder.setSortRequest(sortRequestBuilder.build());

		return requestBuilder.build();
	}

	@Override
	public QueryResult execute(LumongoConnection lumongoConnection) throws ServiceException {

		ExternalService.BlockingInterface service = lumongoConnection.getService();

		RpcController controller = lumongoConnection.getController();

		QueryResponse queryResponse = service.query(controller, getRequest());

		return new QueryResult(queryResponse);

	}

	@Override
	public String toString() {
		return "Query{" + "query='" + query + '\'' + ", amount=" + amount + ", indexes=" + indexes + ", lastResult=" + lastResult + ", countRequests="
				+ countRequests + ", drillDowns=" + drillDowns + ", fieldSorts=" + fieldSorts + ", queryFields=" + queryFields + ", filterQueries="
				+ filterQueries + ", minimumNumberShouldMatch=" + minimumNumberShouldMatch + ", defaultOperator=" + defaultOperator + ", resultFetchType="
				+ resultFetchType + ", documentFields=" + documentFields + ", documentMaskedFields=" + documentMaskedFields + '}';
	}
}
