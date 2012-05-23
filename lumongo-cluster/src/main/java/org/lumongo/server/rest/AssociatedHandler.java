package org.lumongo.server.rest;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.ws.http.HTTPException;

import org.apache.log4j.Logger;
import org.lumongo.server.indexing.IndexManager;
import org.lumongo.util.StreamHelper;

import com.google.common.collect.Multimap;
import com.sun.net.httpserver.HttpExchange;

public class AssociatedHandler extends SimpleHttpHandler {
	
	@SuppressWarnings("unused")
	private final static Logger log = Logger.getLogger(AssociatedHandler.class);
	
	public static final String UNIQUE_ID = "uniqueId";
	public static final String FILE_NAME = "fileName";
	
	private IndexManager indexManager;
	
	public AssociatedHandler(IndexManager indexManager) {
		this.indexManager = indexManager;
	}
	
	@Override
	public void get(HttpExchange exchange) throws IOException {
		try {
			Multimap<String, String> params = this.getUrlParameters(exchange);
			
			if (!params.containsKey(UNIQUE_ID) || !params.containsKey(FILE_NAME)) {
				writeResponse(exchange, UNIQUE_ID + " and " + FILE_NAME + " are required");
				throw new HTTPException(BAD_REQUEST);
			}
			
			String uniqueId = params.get(UNIQUE_ID).iterator().next();
			String fileName = params.get(FILE_NAME).iterator().next();
			
			if (uniqueId != null && fileName != null) {
				InputStream is = indexManager.getAssociatedDocumentStream(uniqueId, fileName);
				if (is != null) {
					StreamHelper.copyStream(is, exchange.getResponseBody());
					return;
				}
			}
			
			writeResponse(exchange, "Cannot find associated document with uniqueId <" + uniqueId + "> with fileName <" + fileName + ">");
			throw new HTTPException(NOT_FOUND);
		}
		finally {
			exchange.close();
		}
		
	}
	
	@Override
	public void post(HttpExchange exchange) throws IOException {
		Multimap<String, String> params = this.getUrlParameters(exchange);
		
		if (!params.containsKey(UNIQUE_ID) || !params.containsKey(FILE_NAME)) {
			throw new IOException(UNIQUE_ID + " and " + FILE_NAME + " are required");
		}
		
		String uniqueId = params.get(UNIQUE_ID).iterator().next();
		String fileName = params.get(FILE_NAME).iterator().next();
		
		if (uniqueId != null && fileName != null) {
			try {
				indexManager.storeAssociatedDocument(uniqueId, fileName, exchange.getRequestBody(), false, null);
			}
			catch (Exception e) {
				writeResponse(exchange, e.getMessage());
				throw new HTTPException(INTERNAL_ERROR);
			}
		}
		
	}
}
