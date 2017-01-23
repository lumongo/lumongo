package org.lumongo.server.rest;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.lumongo.LumongoConstants;
import org.lumongo.server.index.LumongoIndexManager;
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
import java.util.HashMap;
import java.util.List;

@Path(LumongoConstants.ASSOCIATED_DOCUMENTS_URL)
public class AssociatedResource {

	private final static Logger log = Logger.getLogger(AssociatedResource.class);

	private LumongoIndexManager indexManager;

	public AssociatedResource(LumongoIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_OCTET_STREAM })
	public Response get(@Context Response response, @QueryParam(LumongoConstants.ID) final String uniqueId,
			@QueryParam(LumongoConstants.FILE_NAME) final String fileName, @QueryParam(LumongoConstants.INDEX) final String indexName) {

		StreamingOutput stream = new StreamingOutput() {

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
					throw new WebApplicationException(LumongoConstants.ID + " and " + LumongoConstants.FILE_NAME + " are required",
							LumongoConstants.BAD_REQUEST);
				}
			}

		};

		return Response.ok(stream).header("content-disposition", "attachment; filename = " + fileName).build();

	}

	@POST
	@Produces({ MediaType.TEXT_XML })
	public Response post(@QueryParam(LumongoConstants.ID) String uniqueId, @QueryParam(LumongoConstants.FILE_NAME) String fileName,
			@QueryParam(LumongoConstants.INDEX) String indexName, @QueryParam(LumongoConstants.COMPRESSED) Boolean compressed,
			@QueryParam(LumongoConstants.META) List<String> meta, InputStream is) {
		if (uniqueId != null && fileName != null && indexName != null) {

			HashMap<String, String> metaMap = new HashMap<>();
			if (meta != null) {
				for (String m : meta) {
					int colonIndex = m.indexOf(":");
					if (colonIndex != -1) {
						String key = m.substring(0, colonIndex);
						String value = m.substring(colonIndex + 1).trim();
						metaMap.put(key, value);
					}
					else {
						throw new WebApplicationException("Meta must be in the form key:value");
					}
				}
			}

			try {

				if (compressed == null) {
					compressed = false;
				}

				indexManager.storeAssociatedDocument(indexName, uniqueId, fileName, is, compressed, metaMap);

				return Response.status(LumongoConstants.SUCCESS)
						.entity("Stored associated document with uniqueId <" + uniqueId + "> and fileName <" + fileName + ">").build();
			}
			catch (Exception e) {
				log.error(e.getClass().getSimpleName() + ": ", e);
				return Response.status(LumongoConstants.INTERNAL_ERROR).entity(e.getMessage()).build();
			}
		}
		else {
			throw new WebApplicationException(LumongoConstants.ID + " and " + LumongoConstants.FILE_NAME + " are required", LumongoConstants.BAD_REQUEST);
		}

	}

	@GET
	@Path("/all")
	@Produces({ MediaType.APPLICATION_JSON })
	public Response get(@QueryParam(LumongoConstants.INDEX) final String indexName, @QueryParam(LumongoConstants.QUERY) String query) {

		StreamingOutput stream = new StreamingOutput() {

			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {

				Document filter;
				if (query != null) {
					filter = Document.parse(query);
				}
				else {
					filter = new Document();
				}

				indexManager.getAssociatedDocuments(indexName, output, filter);
			}

		};

		return Response.ok(stream).build();

	}
}