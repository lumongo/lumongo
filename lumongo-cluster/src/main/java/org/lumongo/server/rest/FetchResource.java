package org.lumongo.server.rest;

import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.apache.log4j.Logger;
import org.bson.BSON;
import org.lumongo.LumongoConstants;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.server.indexing.LumongoIndexManager;
import org.lumongo.util.StreamHelper;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Path(LumongoConstants.FETCH_URL)
public class FetchResource {


	private LumongoIndexManager indexManager;

	public FetchResource(LumongoIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public Response get(@Context Response response, @QueryParam(LumongoConstants.UNIQUE_ID) final String uniqueId,
					@QueryParam(LumongoConstants.INDEX) final String indexName) {


		Lumongo.FetchRequest.Builder fetchRequest = Lumongo.FetchRequest.newBuilder();
		fetchRequest.setIndexName(indexName);
		fetchRequest.setUniqueId(uniqueId);

		Lumongo.FetchResponse fetchResponse;


		try {
			fetchResponse = indexManager.fetch(fetchRequest.build());

			if (fetchResponse.hasResultDocument()) {
				Lumongo.ResultDocument resultDocument = fetchResponse.getResultDocument();
				ByteString bs = resultDocument.getDocument();

				DBObject document = new BasicDBObject();
				document.putAll(BSON.decode(bs.toByteArray()));
				return Response.status(LumongoConstants.SUCCESS)
								.entity(document.toString()).build();
			}
			else {
				return Response.status(LumongoConstants.NOT_FOUND).entity("Failed to fetch uniqueId <" + uniqueId + "> for index <" + indexName + ">").build();
			}

		}
		catch (Exception e) {
			return Response.status(LumongoConstants.INTERNAL_ERROR).entity("Failed to fetch uniqueId <" + uniqueId + "> for index <" + indexName + ">: " + e.getMessage()).build();
		}

	}


}