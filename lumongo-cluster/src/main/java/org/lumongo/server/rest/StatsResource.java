package org.lumongo.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import org.bson.Document;
import org.lumongo.LumongoConstants;
import org.lumongo.server.index.LumongoIndexManager;
import org.lumongo.storage.lucene.MongoFile;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path(LumongoConstants.STATS_URL)
public class StatsResource {

	private static final int MB = 1024 * 1024;

	private LumongoIndexManager indexManager;

	public StatsResource(LumongoIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8" })
	public Response get(@Context Response response, @QueryParam(LumongoConstants.PRETTY) boolean pretty) {

		try {

			Document mongoDocument = new Document();

			mongoDocument.put("blockSize", indexManager.getClusterConfig().getIndexBlockSize());
			mongoDocument.put("maxIndexBlockCount", indexManager.getClusterConfig().getMaxIndexBlocks());
			mongoDocument.put("currentIndexBlockCount", MongoFile.getCacheSize());

			Runtime runtime = Runtime.getRuntime();

			mongoDocument.put("jvmUsedMemoryMB", (runtime.totalMemory() - runtime.freeMemory()) / MB);
			mongoDocument.put("jvmFreeMemoryMB", runtime.freeMemory() / MB);
			mongoDocument.put("jvmTotalMemoryMB", runtime.totalMemory() / MB);
			mongoDocument.put("jvmMaxMemoryMB", runtime.maxMemory() / MB);

			String docString = mongoDocument.toJson();

			if (pretty) {
				docString = JsonWriter.formatJson(docString);
			}

			return Response.status(LumongoConstants.SUCCESS).entity(docString).build();

		}
		catch (Exception e) {
			return Response.status(LumongoConstants.INTERNAL_ERROR).entity("Failed to get cluster membership: " + e.getMessage()).build();
		}

	}

}