package org.lumongo.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.BSON;
import org.bson.BSONObject;
import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.CountRequest;
import org.lumongo.cluster.message.Lumongo.FacetRequest;
import org.lumongo.cluster.message.Lumongo.LMFacet;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.server.index.LumongoIndexManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path(LumongoConstants.QUERY_URL)
public class QueryResource {

	private LumongoIndexManager indexManager;

	public QueryResource(LumongoIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public String get(
					@QueryParam(LumongoConstants.INDEX) List<String> indexName,
					@QueryParam(LumongoConstants.QUERY) String query,
					@QueryParam(LumongoConstants.QUERY_FIELD) List<String> queryFields,
					@QueryParam(LumongoConstants.FILTER_QUERY) List<String> filterQueries,
					@QueryParam(LumongoConstants.FIELDS) List<String> fields,
					@QueryParam(LumongoConstants.FETCH) Boolean fetch,
					@QueryParam(LumongoConstants.ROWS) int rows,
					@QueryParam(LumongoConstants.FACET) List<String> facet,
					@QueryParam(LumongoConstants.PRETTY) boolean pretty,
					@QueryParam(LumongoConstants.FORMAT) String format) {


		QueryRequest.Builder qrBuilder = QueryRequest.newBuilder().addAllIndex(indexName);
		qrBuilder.setQuery(query);
		qrBuilder.setAmount(rows);

		if (queryFields != null) {
			for (String queryField : queryFields) {
				qrBuilder.addQueryField(queryField);
			}
		}

		if (filterQueries != null) {
			for (String filterQuery : filterQueries) {
				qrBuilder.addFilterQuery(filterQuery);
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
			CountRequest.Builder countBuilder = CountRequest.newBuilder();
			// TODO handle path
			LMFacet lmFacet = LMFacet.newBuilder().setLabel(f).build();

			frBuilder.addCountRequest(countBuilder.setFacetField(lmFacet));
		}
		qrBuilder.setFacetRequest(frBuilder);



		try {
			QueryResponse qr = indexManager.query(qrBuilder.build());

			String response;
			if ("proto".equalsIgnoreCase(format)) {
				response =  JsonFormat.printer().print(qr);
			}
			else {
				response = getStandardResponse(qr);
			}

			if (pretty) {
				response = JsonWriter.formatJson(response);
			}
			return response;
		}
		catch (Exception e) {
			throw new WebApplicationException(LumongoConstants.UNIQUE_ID + " and " + LumongoConstants.FILE_NAME + " are required",
					LumongoConstants.INTERNAL_ERROR);
		}

	}

	private String getStandardResponse(QueryResponse qr) {
		StringBuilder responseBuilder = new StringBuilder();
		responseBuilder.append("{");
		responseBuilder.append("\"numFound\": ");
		responseBuilder.append(qr.getTotalHits());
		responseBuilder.append(",");
		responseBuilder.append("\"docs\": [");

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
			responseBuilder.append(sr.getUniqueId());
			responseBuilder.append(",");
			responseBuilder.append("\"score\": ");
			responseBuilder.append(sr.getScore());
			responseBuilder.append(",");
			responseBuilder.append("\"indexName\": ");
			responseBuilder.append("\"").append(sr.getIndexName()).append("\"");

			if (sr.hasResultDocument()) {
				responseBuilder.append(",");
				Lumongo.ResultDocument document =sr.getResultDocument();
				ByteString bs = document.getDocument();
				BasicDBObject dbObject = new BasicDBObject();
				dbObject.putAll(BSON.decode(bs.toByteArray()));
				responseBuilder.append("\"document\": ");
				responseBuilder.append(dbObject.toString());

			}


			responseBuilder.append("}");
		}
		responseBuilder.append("]");
		responseBuilder.append("}");

		return responseBuilder.toString();
	}
}
