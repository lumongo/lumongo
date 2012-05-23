package org.lumongo.server.rest;

import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.lumongo.LumongoConstants;
import org.lumongo.server.indexing.IndexManager;

import com.google.common.collect.Multimap;
import com.sun.net.httpserver.HttpExchange;

public class AssociatedHandler extends SimpleHttpHandler {
	
	@SuppressWarnings("unused")
	private final static Logger log = Logger.getLogger(AssociatedHandler.class);
	
	private IndexManager indexManager;
	
	public AssociatedHandler(IndexManager indexManager) {
		this.indexManager = indexManager;
	}
	
	@Override
	public void get(HttpExchange exchange) throws IOException {
		
		Multimap<String, String> params = this.getUrlParameters(exchange);
		
		if (params.containsKey(LumongoConstants.UNIQUE_ID) && params.containsKey(LumongoConstants.FILE_NAME)) {
			String uniqueId = params.get(LumongoConstants.UNIQUE_ID).iterator().next();
			String fileName = params.get(LumongoConstants.FILE_NAME).iterator().next();
			
			if (uniqueId != null && fileName != null) {
				InputStream is = indexManager.getAssociatedDocumentStream(uniqueId, fileName);
				if (is != null) {
					writeResponse(LumongoConstants.SUCCESS, exchange, is);
				}
				else {
					writeResponse(LumongoConstants.NOT_FOUND, exchange, "Cannot find associated document with uniqueId <" + uniqueId + "> with fileName <"
							+ fileName + ">");
				}
				return;
			}
		}
		
		writeResponse(LumongoConstants.BAD_REQUEST, exchange, LumongoConstants.UNIQUE_ID + " and " + LumongoConstants.FILE_NAME + " are required");
		
	}
	
	@Override
	public void post(HttpExchange exchange) throws IOException {
		Multimap<String, String> params = this.getUrlParameters(exchange);
		
		if (!params.containsKey(LumongoConstants.UNIQUE_ID) || !params.containsKey(LumongoConstants.FILE_NAME)) {
			throw new IOException(LumongoConstants.UNIQUE_ID + " and " + LumongoConstants.FILE_NAME + " are required");
		}
		
		String uniqueId = params.get(LumongoConstants.UNIQUE_ID).iterator().next();
		String fileName = params.get(LumongoConstants.FILE_NAME).iterator().next();
		
		if (uniqueId != null && fileName != null) {
			try {
				indexManager.storeAssociatedDocument(uniqueId, fileName, exchange.getRequestBody(), false, null);
				writeResponse(LumongoConstants.SUCCESS, exchange, "Stored associated document with uniqueId <" + uniqueId + "> and fileName <" + fileName + ">");
			}
			catch (Exception e) {
				writeResponse(LumongoConstants.INTERNAL_ERROR, exchange, e.getMessage());
			}
		}
		
	}
}
