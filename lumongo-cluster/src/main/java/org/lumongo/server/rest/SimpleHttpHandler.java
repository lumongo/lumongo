package org.lumongo.server.rest;

import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import javax.xml.ws.http.HTTPException;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class SimpleHttpHandler implements HttpHandler {
	
	public static final String WGET_AGENT = "Wget";
	public static final String USER_AGENT = "User-agent";
	public static final int BAD_REQUEST = 400;
	public static final int NOT_FOUND = 404;
	public static final int METHOD_NOT_ALLOWED = 405;
	public static final int INTERNAL_ERROR = 500;
	
	public static final String GET = "GET";
	public static final String POST = "POST";
	public static final String HEAD = "HEAD";
	public static final String PUT = "PUT";
	public static final String DELETE = "DELETE";
	
	@Override
	public void handle(HttpExchange exchange) throws IOException {
		if (GET.equals(exchange.getRequestMethod())) {
			get(exchange);
		}
		else if (POST.equals(exchange.getRequestMethod())) {
			post(exchange);
		}
		else if (HEAD.equals(exchange.getRequestMethod())) {
			head(exchange);
		}
		else if (PUT.equals(exchange.getRequestMethod())) {
			put(exchange);
		}
		else if (DELETE.equals(exchange.getRequestMethod())) {
			delete(exchange);
		}
		else {
			throw new HTTPException(METHOD_NOT_ALLOWED);
		}
	}
	
	public void get(HttpExchange exchange) throws IOException {
		throw new HTTPException(METHOD_NOT_ALLOWED);
	}
	
	public void post(HttpExchange exchange) throws IOException {
		throw new HTTPException(METHOD_NOT_ALLOWED);
	}
	
	public void head(HttpExchange exchange) throws IOException {
		throw new HTTPException(METHOD_NOT_ALLOWED);
	}
	
	public void put(HttpExchange exchange) throws IOException {
		throw new HTTPException(METHOD_NOT_ALLOWED);
	}
	
	public void delete(HttpExchange exchange) throws IOException {
		throw new HTTPException(METHOD_NOT_ALLOWED);
	}
	
	protected String getRequestPath(HttpExchange ex) throws IOException {
		return ex.getRequestURI().getPath();
	}
	
	protected Multimap<String, String> getUrlParameters(HttpExchange ex) {
		String query = ex.getRequestURI().getQuery();
		if (query != null) {
			Scanner s = new Scanner(query);
			return parse(s);
		}
		return ArrayListMultimap.create();
	}
	
	protected Multimap<String, String> getBodyParameters(HttpExchange ex) {
		Scanner s = new Scanner(ex.getRequestBody(), "UTF-8");
		return parse(s);
	}
	
	private Multimap<String, String> parse(Scanner s) {
		Multimap<String, String> ret = ArrayListMultimap.create();
		
		s.useDelimiter("&");
		
		while (s.hasNext()) {
			String token = s.next();
			int i = token.indexOf("=");
			if (i != -1) {
				ret.put(token.substring(0, i).trim(), token.substring(i + 1).trim());
			}
			else {
				ret.put(token.trim(), null);
			}
			
		}
		
		return ret;
	}
	
	protected void writeResponse(HttpExchange ex, String text) throws IOException {
		writeResponse(ex, text.getBytes("UTF-8"));
	}
	
	protected void writeResponse(HttpExchange ex, byte[] bytes) throws IOException {
		try {
			
			List<String> agents = ex.getRequestHeaders().get(USER_AGENT);
			
			//disable chunk encoding for wget
			if (agents != null && agents.size() == 1 && agents.get(0).contains(WGET_AGENT)) {
				ex.sendResponseHeaders(200, bytes.length);
			}
			else {
				ex.sendResponseHeaders(200, 0);
			}
			
			ex.getResponseBody().write(bytes);
		}
		finally {
			ex.close();
		}
	}
}
