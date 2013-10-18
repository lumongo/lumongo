package org.lumongo;

import com.google.common.base.Joiner;

public class LumongoConstants {
	
	public final static int DEFAULT_HAZELCAST_PORT = 5701;
	public final static int DEFAULT_INTERNAL_SERVICE_PORT = 32190;
	public final static int DEFAULT_EXTERNAL_SERVICE_PORT = 32191;
	public final static int DEFAULT_REST_SERVICE_PORT = 32192;
	
	//HTTP constants
	public static final int SUCCESS = 200;
	public static final int BAD_REQUEST = 400;
	public static final int NOT_FOUND = 404;
	public static final int METHOD_NOT_ALLOWED = 405;
	public static final int INTERNAL_ERROR = 500;
	
	public static final String GET = "GET";
	public static final String POST = "POST";
	public static final String HEAD = "HEAD";
	public static final String PUT = "PUT";
	public static final String DELETE = "DELETE";
	
	public static final String ASSOCIATED_DOCUMENTS_URL = "/associatedDocs";
	
	public static final String UNIQUE_ID = "uniqueId";
	public static final String FILE_NAME = "fileName";
	public static final String INDEX_NAME = "indexName";
	
	//General
	public static final String UTF8 = "UTF-8";
	
	public static final char FACET_DELIMITER = '/';
	
	public static final Joiner FACET_JOINER = Joiner.on(FACET_DELIMITER);
	
	public static final String TIMESTAMP_FIELD = "lmtsf";
	public static final String LUCENE_FACET_FIELD = "$facets";
}
