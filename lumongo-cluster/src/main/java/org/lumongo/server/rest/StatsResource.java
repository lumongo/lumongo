package org.lumongo.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
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

			DBObject document = new BasicDBObject();

			document.put("blockSize", indexManager.getClusterConfig().getIndexBlockSize());
			document.put("maxIndexBlockCount", indexManager.getClusterConfig().getMaxIndexBlocks());
			document.put("currentIndexBlockCount", MongoFile.getCacheSize());

			Runtime runtime = Runtime.getRuntime();

			document.put("jvmUsedMemoryMB", (runtime.totalMemory() - runtime.freeMemory()) / MB);
			document.put("jvmFreeMemoryMB",  runtime.freeMemory() / MB);
			document.put("jvmTotalMemoryMB", runtime.totalMemory() / MB);
			document.put("jvmMaxMemoryMB", runtime.maxMemory() / MB);

			String docString = document.toString();

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