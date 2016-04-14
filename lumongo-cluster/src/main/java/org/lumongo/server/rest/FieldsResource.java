package org.lumongo.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
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

@Path(LumongoConstants.FIELDS_URL)
public class FieldsResource {

	private LumongoIndexManager indexManager;

	public FieldsResource(LumongoIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public Response get(@Context Response response, @QueryParam(LumongoConstants.INDEX) final String indexName,
			@QueryParam(LumongoConstants.PRETTY) boolean pretty) {

		if (indexName != null) {

			Lumongo.GetFieldNamesRequest fieldNamesRequest = Lumongo.GetFieldNamesRequest.newBuilder().setIndexName(indexName).build();

			Lumongo.GetFieldNamesResponse fieldNamesResponse;

			try {
				fieldNamesResponse = indexManager.getFieldNames(fieldNamesRequest);

				DBObject document = new BasicDBObject();
				document.put("index", indexName);
				document.put("fields", fieldNamesResponse.getFieldNameList());
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
			return Response.status(LumongoConstants.INTERNAL_ERROR).entity("No index defined").build();
		}

	}

}