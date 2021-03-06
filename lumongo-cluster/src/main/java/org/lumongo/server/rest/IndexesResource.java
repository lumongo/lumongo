package org.lumongo.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import com.mongodb.util.JSONSerializers;
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

@Path(LumongoConstants.INDEXES_URL)
public class IndexesResource {

	private LumongoIndexManager indexManager;

	public IndexesResource(LumongoIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public Response get(@Context Response response, @QueryParam(LumongoConstants.PRETTY) boolean pretty) {

		try {
			Lumongo.GetIndexesResponse getIndexesResponse = indexManager.getIndexes(Lumongo.GetIndexesRequest.newBuilder().build());

			Document mongoDocument = new org.bson.Document();
			mongoDocument.put("indexes", getIndexesResponse.getIndexNameList());
			String docString = JSONSerializers.getStrict().serialize(mongoDocument);

			if (pretty) {
				docString = JsonWriter.formatJson(docString);
			}

			return Response.status(LumongoConstants.SUCCESS).entity(docString).build();

		}
		catch (Exception e) {
			return Response.status(LumongoConstants.INTERNAL_ERROR).entity("Failed to get index names: " + e.getMessage()).build();
		}

	}

}