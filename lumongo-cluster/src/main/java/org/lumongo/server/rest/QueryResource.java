package org.lumongo.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.mongodb.util.JSONSerializers;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.AnalyzerSettings.Similarity;
import org.lumongo.cluster.message.Lumongo.CountRequest;
import org.lumongo.cluster.message.Lumongo.FacetRequest;
import org.lumongo.cluster.message.Lumongo.FieldSimilarity;
import org.lumongo.cluster.message.Lumongo.LMFacet;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.server.index.LumongoIndexManager;
import org.lumongo.util.LumongoUtil;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path(LumongoConstants.QUERY_URL)
public class QueryResource {

	private final static Logger log = Logger.getLogger(QueryResource.class);

	private LumongoIndexManager indexManager;

	public QueryResource(LumongoIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public Response get(@QueryParam(LumongoConstants.INDEX) List<String> indexName, @QueryParam(LumongoConstants.QUERY) String query,
			@QueryParam(LumongoConstants.QUERY_FIELD) List<String> queryFields, @QueryParam(LumongoConstants.FILTER_QUERY) List<String> filterQueries,
			@QueryParam(LumongoConstants.FILTER_QUERY_JSON) List<String> filterJsonQueries, @QueryParam(LumongoConstants.FIELDS) List<String> fields,
			@QueryParam(LumongoConstants.FETCH) Boolean fetch, @QueryParam(LumongoConstants.ROWS) int rows,
			@QueryParam(LumongoConstants.FACET) List<String> facet, @QueryParam(LumongoConstants.DRILL_DOWN) List<String> drillDowns,
			@QueryParam(LumongoConstants.DEFAULT_OP) String defaultOperator, @QueryParam(LumongoConstants.SORT) List<String> sort,
			@QueryParam(LumongoConstants.PRETTY) boolean pretty, @QueryParam(LumongoConstants.COMPUTE_FACET_ERROR) boolean computeFacetError,
			@QueryParam(LumongoConstants.DISMAX) Boolean dismax, @QueryParam(LumongoConstants.DISMAX_TIE) Float dismaxTie,
			@QueryParam(LumongoConstants.MIN_MATCH) Integer mm, @QueryParam(LumongoConstants.SIMILARITY) List<String> similarity) {

		QueryRequest.Builder qrBuilder = QueryRequest.newBuilder().addAllIndex(indexName);
		if (query != null && !query.isEmpty()) {
			Lumongo.Query.Builder queryBuilder = Lumongo.Query.newBuilder();
			queryBuilder.setQ(query);
			if (mm != null) {
				queryBuilder.setMm(mm);
			}
			if (dismax != null) {
				queryBuilder.setDismax(dismax);
				if (dismaxTie != null) {
					queryBuilder.setDismaxTie(dismaxTie);
				}
			}
			if (!queryFields.isEmpty()) {
				queryBuilder.addAllQf(queryFields);
			}
			if (defaultOperator != null) {
				if (defaultOperator.equalsIgnoreCase("AND")) {
					queryBuilder.setDefaultOp(Lumongo.Query.Operator.AND);
				}
				else if (defaultOperator.equalsIgnoreCase("OR")) {
					queryBuilder.setDefaultOp(Lumongo.Query.Operator.OR);
				}
				else {
					Response.status(LumongoConstants.INTERNAL_ERROR).entity("Invalid default operator <" + defaultOperator + ">").build();
				}
			}

			qrBuilder.setQuery(queryBuilder);

			if (similarity != null) {
				for (String sim : similarity) {
					if (sim.contains(":")) {
						int i = sim.indexOf(":");
						String field = sim.substring(0, i);
						String simType = sim.substring(i + 1);

						FieldSimilarity.Builder fieldSimilarity = FieldSimilarity.newBuilder();
						fieldSimilarity.setField(field);

						if (simType.equalsIgnoreCase("bm25")) {
							fieldSimilarity.setSimilarity(Similarity.BM25);
						}
						else if (simType.equalsIgnoreCase("constant")) {
							fieldSimilarity.setSimilarity(Similarity.CONSTANT);
						}
						else if (simType.equalsIgnoreCase("tf")) {
							fieldSimilarity.setSimilarity(Similarity.TF);
						}
						else if (simType.equalsIgnoreCase("tfidf")) {
							fieldSimilarity.setSimilarity(Similarity.TFIDF);
						}
						else {
							Response.status(LumongoConstants.INTERNAL_ERROR).entity("Unknown similarity type <" + simType + ">").build();
						}

						qrBuilder.addFieldSimilarity(fieldSimilarity);
					}
					else {
						Response.status(LumongoConstants.INTERNAL_ERROR).entity("Similarity <" + sim + "> should be in the form field:simType").build();
					}
				}
			}
		}
		qrBuilder.setAmount(rows);

		if (filterQueries != null) {
			for (String filterQuery : filterQueries) {
				Lumongo.Query filterQueryBuilder = Lumongo.Query.newBuilder().setQ(filterQuery).build();
				qrBuilder.addFilterQuery(filterQueryBuilder);
			}
		}

		if (filterJsonQueries != null) {
			for (String filterJsonQuery : filterJsonQueries) {
				try {
					Lumongo.Query.Builder filterQueryBuilder = Lumongo.Query.newBuilder();
					JsonFormat.parser().merge(filterJsonQuery, filterQueryBuilder);
					qrBuilder.addFilterQuery(filterQueryBuilder);
				}
				catch (InvalidProtocolBufferException e) {
					return Response.status(LumongoConstants.INTERNAL_ERROR).entity(e.getClass().getSimpleName() + ":" + e.getMessage()).build();
				}
			}
		}

		if (fields != null) {
			for (String field : fields) {
				if (field.startsWith("-")) {
					qrBuilder.addDocumentMaskedFields(field.substring(1, field.length()));
				}
				else {
					qrBuilder.addDocumentFields(field);
				}
			}
		}

		qrBuilder.setResultFetchType(Lumongo.FetchType.FULL);
		if (fetch != null && !fetch) {
			qrBuilder.setResultFetchType(Lumongo.FetchType.NONE);
		}

		FacetRequest.Builder frBuilder = FacetRequest.newBuilder();
		for (String f : facet) {
			Integer count = null;
			if (f.contains(":")) {
				String countString = f.substring(f.indexOf(":") + 1);
				f = f.substring(0, f.indexOf(":"));
				try {
					count = Integer.parseInt(countString);
				}
				catch (Exception e) {
					Response.status(LumongoConstants.INTERNAL_ERROR).entity("Invalid facet count <" + countString + "> for facet <" + f + ">").build();
				}
			}

			CountRequest.Builder countBuilder = CountRequest.newBuilder();
			LMFacet lmFacet = LMFacet.newBuilder().setLabel(f).build();
			CountRequest.Builder facetBuilder = countBuilder.setFacetField(lmFacet);
			if (count != null) {
				facetBuilder.setMaxFacets(count);
			}
			if (computeFacetError) {
				facetBuilder.setComputeError(true);
				facetBuilder.setComputePossibleMissed(true);
			}
			frBuilder.addCountRequest(facetBuilder);
		}
		if (drillDowns != null) {
			for (String drillDown : drillDowns) {
				if (drillDown.contains(":")) {
					String value = drillDown.substring(drillDown.indexOf(":") + 1);
					String field = drillDown.substring(0, drillDown.indexOf(":"));
					frBuilder.addDrillDown(LMFacet.newBuilder().setLabel(field).setPath(value));
				}
			}
		}

		qrBuilder.setFacetRequest(frBuilder);

		Lumongo.SortRequest.Builder sortRequest = Lumongo.SortRequest.newBuilder();
		for (String sortField : sort) {

			Lumongo.FieldSort.Builder fieldSort = Lumongo.FieldSort.newBuilder();
			if (sortField.contains(":")) {
				String sortDir = sortField.substring(sortField.indexOf(":") + 1);
				sortField = sortField.substring(0, sortField.indexOf(":"));

				if ("-1".equals(sortDir) || "DESC".equalsIgnoreCase(sortDir)) {
					fieldSort.setDirection(Lumongo.FieldSort.Direction.DESCENDING);
				}
				else if ("1".equals(sortDir) || "ASC".equalsIgnoreCase(sortDir)) {
					fieldSort.setDirection(Lumongo.FieldSort.Direction.ASCENDING);
				}
				else {
					Response.status(LumongoConstants.INTERNAL_ERROR)
							.entity("Invalid sort direction <" + sortDir + "> for field <" + sortField + ">.  Expecting -1/1 or DESC/ASC").build();
				}
			}
			fieldSort.setSortField(sortField);
			sortRequest.addFieldSort(fieldSort);
		}
		qrBuilder.setSortRequest(sortRequest);

		try {
			QueryResponse qr = indexManager.query(qrBuilder.build());

			String response = getStandardResponse(qr);

			if (pretty) {
				response = JsonWriter.formatJson(response);
			}

			return Response.status(LumongoConstants.SUCCESS).entity(response).build();
		}
		catch (Exception e) {
			log.error(e.getClass().getSimpleName() + ":", e);
			return Response.status(LumongoConstants.INTERNAL_ERROR).entity(e.getClass().getSimpleName() + ":" + e.getMessage()).build();
		}

	}

	private String getStandardResponse(QueryResponse qr) throws InvalidProtocolBufferException {
		StringBuilder responseBuilder = new StringBuilder();
		responseBuilder.append("{");
		responseBuilder.append("\"totalHits\": ");
		responseBuilder.append(qr.getTotalHits());

		if (!qr.getResultsList().isEmpty()) {
			responseBuilder.append(",");
			responseBuilder.append("\"results\": [");
			boolean first = true;
			for (Lumongo.ScoredResult sr : qr.getResultsList()) {
				if (first) {
					first = false;
				}
				else {
					responseBuilder.append(",");
				}
				responseBuilder.append("{");
				responseBuilder.append("\"id\": ");
				responseBuilder.append("\"").append(sr.getUniqueId()).append("\"");
				responseBuilder.append(",");
				responseBuilder.append("\"score\": ");
				responseBuilder.append(sr.getScore());
				responseBuilder.append(",");
				responseBuilder.append("\"indexName\": ");
				responseBuilder.append("\"").append(sr.getIndexName()).append("\"");

				if (sr.hasResultDocument()) {
					responseBuilder.append(",");

					Document document = LumongoUtil.resultDocumentToMongoDocument(sr.getResultDocument());
					responseBuilder.append("\"document\": ");
					responseBuilder.append(JSONSerializers.getStrict().serialize(document));

				}

				responseBuilder.append("}");
			}
			responseBuilder.append("]");
		}

		if (!qr.getFacetGroupList().isEmpty()) {
			responseBuilder.append(",");
			responseBuilder.append("\"facets\": [");
			boolean first = true;
			for (Lumongo.FacetGroup facetGroup : qr.getFacetGroupList()) {
				if (first) {
					first = false;
				}
				else {
					responseBuilder.append(",");
				}
				responseBuilder.append("{");
				responseBuilder.append("\"field\": \"");
				responseBuilder.append(facetGroup.getCountRequest().getFacetField().getLabel());
				responseBuilder.append("\"");
				if (facetGroup.hasPossibleMissing()) {
					responseBuilder.append(",");
					responseBuilder.append("\"maxPossibleMissing\": ");
					responseBuilder.append(facetGroup.getMaxValuePossibleMissing());
				}
				responseBuilder.append(",");
				responseBuilder.append("\"values\": [");


				JsonFormat.Printer printer = JsonFormat.printer();

				boolean firstInner = true;
				for (Lumongo.FacetCount facetCount : facetGroup.getFacetCountList()) {
					if (firstInner) {
						firstInner = false;
					}
					else {
						responseBuilder.append(",");
					}


					responseBuilder.append(printer.print(facetCount));
				}
				responseBuilder.append("]");

				responseBuilder.append("}");
			}
			responseBuilder.append("]");
		}

		responseBuilder.append("}");

		return responseBuilder.toString();
	}
}
