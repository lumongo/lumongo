package org.lumongo.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.Document;
import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.server.index.LumongoIndexManager;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

@Path(LumongoConstants.TERMS_URL)
public class TermsResource {

	private LumongoIndexManager indexManager;

	public TermsResource(LumongoIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public Response get(@Context Response response, @QueryParam(LumongoConstants.INDEX) final String indexName,
			@QueryParam(LumongoConstants.FIELDS) final String field,
			@QueryParam(LumongoConstants.AMOUNT) final Integer amount,
			@QueryParam(LumongoConstants.MIN_DOC_FREQ) final Integer minDocFreq,
			@QueryParam(LumongoConstants.MIN_TERM_FREQ) final Integer minTermFreq,
			@QueryParam(LumongoConstants.START_TERM) final String startTerm,
			@QueryParam(LumongoConstants.END_TERM) final String endTerm,
			@QueryParam(LumongoConstants.PRETTY) boolean pretty) {

		if (indexName != null && field != null) {

			Lumongo.GetTermsRequest.Builder termsBuilder = Lumongo.GetTermsRequest.newBuilder();
			termsBuilder.setIndexName(indexName);
			termsBuilder.setFieldName(field);
			if (amount != null) {
				termsBuilder.setAmount(amount);
			}

			if (minDocFreq != null) {
				termsBuilder.setMinDocFreq(minDocFreq);
			}
			if (minTermFreq != null) {
				termsBuilder.setMinTermFreq(minTermFreq);
			}

			if (startTerm != null) {
				termsBuilder.setStartTerm(startTerm);
			}
			if (endTerm != null) {
				termsBuilder.setEndTerm(endTerm);
			}


			try {
				Lumongo.GetTermsResponse terms = indexManager.getTerms(termsBuilder.build());

				BasicDBObject document = new BasicDBObject();
				document.put("index", indexName);
				document.put("field", field);

				List<BasicDBObject> termsDocs = new ArrayList<>();
				for (Lumongo.Term term : terms.getTermList()) {
					BasicDBObject termDoc = new BasicDBObject();
					termDoc.put("term", term.getValue());
					termDoc.put("docFreq", term.getDocFreq());
					termDoc.put("termFreq", term.getTermFreq());
					termsDocs.add(termDoc);
				}


				document.put("terms", termsDocs);
				String docString = document.toString();

				if (pretty) {
					docString = JsonWriter.formatJson(docString);
				}

				return Response.status(LumongoConstants.SUCCESS).entity(docString).build();

			}
			catch (Exception e) {
				return Response.status(LumongoConstants.INTERNAL_ERROR).entity("Failed to fetch fields for index <" + indexName + ">: " + e.getMessage())
						.build();
			}
		}
		else {
			return Response.status(LumongoConstants.INTERNAL_ERROR).entity("No index or field defined").build();
		}

	}

}