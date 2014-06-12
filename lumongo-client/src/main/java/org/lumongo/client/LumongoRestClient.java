package org.lumongo.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.HashMap;

import org.lumongo.LumongoConstants;
import org.lumongo.util.HttpHelper;
import org.lumongo.util.StreamHelper;

public class LumongoRestClient {
	private String server;
	private int restPort;
	
	public LumongoRestClient(String server) {
		this(server, LumongoConstants.DEFAULT_REST_SERVICE_PORT);
	}
	
	public LumongoRestClient(String server, int restPort) {
		this.server = server;
		this.restPort = restPort;
	}
	
	public void fetchAssociated(String uniqueId, String indexName, String fileName, File outputFile) throws IOException {
		fetchAssociated(uniqueId, indexName, fileName, new FileOutputStream(outputFile));
	}
	
	public void fetchAssociated(String uniqueId, String indexName, String fileName, OutputStream destination) throws IOException {
		InputStream source = null;
		HttpURLConnection conn = null;
		
		try {
			HashMap<String, String> parameters = new HashMap<String, String>();
			parameters.put(LumongoConstants.UNIQUE_ID, uniqueId);
			parameters.put(LumongoConstants.FILE_NAME, fileName);
			parameters.put(LumongoConstants.INDEX, indexName);
			
			String url = HttpHelper.createRequestUrl(server, restPort, LumongoConstants.ASSOCIATED_DOCUMENTS_URL, parameters);
			conn = createGetConnection(url);
			
			if (conn.getResponseCode() != LumongoConstants.SUCCESS) {
				byte[] bytes = StreamHelper.getBytesFromStream(conn.getErrorStream());
				throw new IOException("Request failed with <" + conn.getResponseCode() + ">: " + new String(bytes, LumongoConstants.UTF8));
			}
			
			source = conn.getInputStream();
			StreamHelper.copyStream(source, destination);
		}
		finally {
			closeStreams(source, destination, conn);
		}
	}
	
	public void storeAssociated(String uniqueId, String indexName, String fileName, File fileToStore) throws IOException {
		storeAssociated(uniqueId, indexName, fileName, new FileInputStream(fileToStore));
	}
	
	public void storeAssociated(String uniqueId, String indexName, String fileName, InputStream source) throws IOException {
		HttpURLConnection conn = null;
		OutputStream destination = null;
		try {
			
			HashMap<String, String> parameters = new HashMap<String, String>();
			parameters.put(LumongoConstants.UNIQUE_ID, uniqueId);
			parameters.put(LumongoConstants.FILE_NAME, fileName);
			parameters.put(LumongoConstants.INDEX, indexName);
			
			String url = HttpHelper.createRequestUrl(server, restPort, LumongoConstants.ASSOCIATED_DOCUMENTS_URL, parameters);
			
			conn = createPostConnection(url);
			
			destination = conn.getOutputStream();
			
			StreamHelper.copyStream(source, destination);
			
			if (conn.getResponseCode() != LumongoConstants.SUCCESS) {
				byte[] bytes = StreamHelper.getBytesFromStream(conn.getErrorStream());
				throw new IOException("Request failed with <" + conn.getResponseCode() + ">: " + new String(bytes, LumongoConstants.UTF8));
			}
		}
		finally {
			closeStreams(source, destination, conn);
		}
	}
	
	private void closeStreams(InputStream source, OutputStream destination, HttpURLConnection conn) {
		if (source != null) {
			try {
				source.close();
			}
			catch (Exception e) {
				
			}
		}
		
		if (destination != null) {
			try {
				destination.close();
			}
			catch (Exception e) {
				
			}
		}
		
		if (conn != null) {
			conn.disconnect();
		}
	}
	
	protected HttpURLConnection createPostConnection(String url) throws IOException, MalformedURLException, ProtocolException {
		HttpURLConnection conn;
		conn = (HttpURLConnection) (new URL(url)).openConnection();
		conn.setDoInput(true);
		conn.setDoOutput(true);
		conn.setRequestMethod(LumongoConstants.POST);
		conn.connect();
		return conn;
	}
	
	protected HttpURLConnection createGetConnection(String url) throws IOException, MalformedURLException, ProtocolException {
		HttpURLConnection conn;
		conn = (HttpURLConnection) (new URL(url)).openConnection();
		conn.setDoInput(true);
		conn.setDoOutput(true);
		conn.setRequestMethod(LumongoConstants.GET);
		conn.connect();
		return conn;
	}
}
