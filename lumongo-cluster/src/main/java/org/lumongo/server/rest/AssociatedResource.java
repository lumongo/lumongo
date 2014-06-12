package org.lumongo.server.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.lumongo.LumongoConstants;
import org.lumongo.server.indexing.IndexManager;
import org.lumongo.util.StreamHelper;

@Path(LumongoConstants.ASSOCIATED_DOCUMENTS_URL)
public class AssociatedResource {
	
	private IndexManager indexManager;
	
	public AssociatedResource(IndexManager indexManager) {
		this.indexManager = indexManager;
	}
	
	@GET
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public StreamingOutput get(@QueryParam(LumongoConstants.UNIQUE_ID) final String uniqueId, @QueryParam(LumongoConstants.FILE_NAME) final String fileName,
					@QueryParam(LumongoConstants.INDEX) final String indexName) {
		return new StreamingOutput() {
			
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				if (uniqueId != null && fileName != null && indexName != null) {
					InputStream is = indexManager.getAssociatedDocumentStream(indexName, uniqueId, fileName);
					if (is != null) {
						StreamHelper.copyStream(is, output);
						
					}
					else {
						throw new WebApplicationException("Cannot find associated document with uniqueId <" + uniqueId + "> with fileName <" + fileName + ">",
										LumongoConstants.NOT_FOUND);
					}
				}
				else {
					throw new WebApplicationException(LumongoConstants.UNIQUE_ID + " and " + LumongoConstants.FILE_NAME + " are required",
									LumongoConstants.BAD_REQUEST);
				}
			}
			
		};
		
	}
	
	@POST
	@Produces({ MediaType.TEXT_XML })
	public Response post(@QueryParam(LumongoConstants.UNIQUE_ID) final String uniqueId, @QueryParam(LumongoConstants.FILE_NAME) final String fileName,
					@QueryParam(LumongoConstants.INDEX) final String indexName, final InputStream is) {
		if (uniqueId != null && fileName != null && indexName != null) {
			
			try {
				indexManager.storeAssociatedDocument(indexName, uniqueId, fileName, is, false, null);
				
				return Response.status(LumongoConstants.SUCCESS)
								.entity("Stored associated document with uniqueId <" + uniqueId + "> and fileName <" + fileName + ">").build();
			}
			catch (Exception e) {
				return Response.status(LumongoConstants.INTERNAL_ERROR).entity(e.getMessage()).build();
			}
		}
		else {
			throw new WebApplicationException(LumongoConstants.UNIQUE_ID + " and " + LumongoConstants.FILE_NAME + " are required", LumongoConstants.BAD_REQUEST);
		}
		
	}
}
