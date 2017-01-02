package org.lumongo.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import com.mongodb.util.JSONSerializers;
import org.bson.Document;
import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.server.index.LumongoIndexManager;
import org.lumongo.util.ResultHelper;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path(LumongoConstants.FETCH_URL)
public class FetchResource {

	private LumongoIndexManager indexManager;

	public FetchResource(LumongoIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public Response get(@Context Response response, @QueryParam(LumongoConstants.ID) final String uniqueId,
			@QueryParam(LumongoConstants.INDEX) final String indexName, @QueryParam(LumongoConstants.PRETTY) boolean pretty) {

		Lumongo.FetchRequest.Builder fetchRequest = Lumongo.FetchRequest.newBuilder();
		fetchRequest.setIndexName(indexName);
		fetchRequest.setUniqueId(uniqueId);

		Lumongo.FetchResponse fetchResponse;

		try {
			fetchResponse = indexManager.fetch(fetchRequest.build());

			if (fetchResponse.hasResultDocument()) {
				Document document = ResultHelper.getDocumentFromResultDocument(fetchResponse.getResultDocument());
				String docString;
				if (pretty) {
					docString = JSONSerializers.getLegacy().serialize(document);
				}
				else {
					docString = JSONSerializers.getStrict().serialize(document);
				}

				if (pretty) {
					docString = JsonWriter.formatJson(docString);
				}

				return Response.status(LumongoConstants.SUCCESS).entity(docString).build();
			}
			else {
				return Response.status(LumongoConstants.NOT_FOUND).entity("Failed to fetch uniqueId <" + uniqueId + "> for index <" + indexName + ">").build();
			}

		}
		catch (Exception e) {
			return Response.status(LumongoConstants.INTERNAL_ERROR)
					.entity("Failed to fetch uniqueId <" + uniqueId + "> for index <" + indexName + ">: " + e.getMessage()).build();
		}

	}

}