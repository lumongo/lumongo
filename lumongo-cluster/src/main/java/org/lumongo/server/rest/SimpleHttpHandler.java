package org.lumongo.server.rest;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

import org.lumongo.LumongoConstants;
import org.lumongo.util.StreamHelper;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class SimpleHttpHandler implements HttpHandler {
	
	@Override
	public void handle(HttpExchange exchange) throws IOException {
		try {
			if (LumongoConstants.GET.equals(exchange.getRequestMethod())) {
				get(exchange);
			}
			else if (LumongoConstants.POST.equals(exchange.getRequestMethod())) {
				post(exchange);
			}
			else if (LumongoConstants.HEAD.equals(exchange.getRequestMethod())) {
				head(exchange);
			}
			else if (LumongoConstants.PUT.equals(exchange.getRequestMethod())) {
				put(exchange);
			}
			else if (LumongoConstants.DELETE.equals(exchange.getRequestMethod())) {
				delete(exchange);
			}
			else {
				sendNotSupported(exchange);
			}
		}
		finally {
			exchange.close();
		}
	}
	
	protected void sendNotSupported(HttpExchange exchange) throws IOException {
		exchange.sendResponseHeaders(LumongoConstants.METHOD_NOT_ALLOWED, -1);
	}
	
	public void get(HttpExchange exchange) throws IOException {
		sendNotSupported(exchange);
	}
	
	public void post(HttpExchange exchange) throws IOException {
		sendNotSupported(exchange);
	}
	
	public void head(HttpExchange exchange) throws IOException {
		sendNotSupported(exchange);
	}
	
	public void put(HttpExchange exchange) throws IOException {
		sendNotSupported(exchange);
	}
	
	public void delete(HttpExchange exchange) throws IOException {
		sendNotSupported(exchange);
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
		Scanner s = new Scanner(ex.getRequestBody(), LumongoConstants.UTF8);
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
	
	protected void writeResponse(int statusCode, HttpExchange exchange, String text) throws IOException {
		writeResponse(statusCode, exchange, text.getBytes("UTF-8"));
	}
	
	protected void writeResponse(int statusCode, HttpExchange exchange, InputStream is) throws IOException {
		try {
			exchange.sendResponseHeaders(statusCode, 0);
			StreamHelper.copyStream(is, exchange.getResponseBody());
		}
		finally {
			exchange.close();
		}
	}
	
	protected void writeResponse(int statusCode, HttpExchange exchange, byte[] bytes) throws IOException {
		
		try {
			exchange.sendResponseHeaders(statusCode, 0);
			exchange.getResponseBody().write(bytes);
			exchange.getResponseBody().flush();
			
		}
		finally {
			exchange.close();
		}
		
	}
}
