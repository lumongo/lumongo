package org.lumongo.server.rest;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.UriBuilder;

import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.indexing.IndexManager;

public class RestServiceManager {
	
	private final static Logger log = Logger.getLogger(RestServiceManager.class);
	
	private final int restPort;
	
	private IndexManager indexManager;
	
	private HttpServer server;
	
	public RestServiceManager(LocalNodeConfig localNodeConfig, IndexManager indexManager) {
		this.indexManager = indexManager;
		this.restPort = localNodeConfig.getRestPort();
		
	}
	
	public void start() throws IOException {
		
		URI baseUri = UriBuilder.fromUri("http://0.0.0.0/").port(restPort).build();
		ResourceConfig config = new ResourceConfig();
		config.register(new AssociatedResource(indexManager));
		server = GrizzlyHttpServerFactory.createHttpServer(baseUri, config);
		
	}
	
	public void shutdown() {
		log.info("Starting rest service shutdown");
		//TODO configure
		server.shutdown(1, TimeUnit.SECONDS);
	}
}
