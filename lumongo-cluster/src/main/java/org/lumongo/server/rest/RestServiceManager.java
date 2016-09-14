package org.lumongo.server.rest;

import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.lumongo.server.config.LocalNodeConfig;
import org.lumongo.server.index.LumongoIndexManager;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

public class RestServiceManager {
	
	private final static Logger log = Logger.getLogger(RestServiceManager.class);
	
	private final int restPort;
	
	private LumongoIndexManager indexManager;
	
	private HttpServer server;
	
	public RestServiceManager(LocalNodeConfig localNodeConfig, LumongoIndexManager indexManager) {
		this.indexManager = indexManager;
		this.restPort = localNodeConfig.getRestPort();
		
	}
	
	public void start() throws IOException {
		
		URI baseUri = UriBuilder.fromUri("http://0.0.0.0/").port(restPort).build();
		ResourceConfig config = new ResourceConfig();
		config.register(new AssociatedResource(indexManager));
		config.register(new QueryResource(indexManager));
		config.register(new FetchResource(indexManager));
		config.register(new FieldsResource(indexManager));
		config.register(new IndexResource(indexManager));
		config.register(new IndexesResource(indexManager));
		config.register(new TermsResource(indexManager));
		config.register(new MembersResource(indexManager));
		config.register(new StatsResource(indexManager));
		server = GrizzlyHttpServerFactory.createHttpServer(baseUri, config);
		
	}
	
	public void shutdown() {
		log.info("Starting rest service shutdown");
		//TODO configure
		server.shutdown(1, TimeUnit.SECONDS);
	}
}
