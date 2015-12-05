package org.lumongo;

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
	public static final String QUERY_URL = "query";
	public static final String FETCH_URL = "fetch";

	public static final String QUERY = "q";
	public static final String QUERY_FIELD = "qf";
	public static final String FILTER_QUERY = "fq";
	public static final String ROWS = "rows";
	public static final String UNIQUE_ID = "uniqueId";
	public static final String FILE_NAME = "fileName";
	public static final String INDEX = "index";
	public static final String FACET = "facet";
	public static final String FETCH = "fetch";
	public static final String FIELDS = "fl";
	public static final String PRETTY = "pretty";
	public static final String FORMAT = "format";
	
	//General
	public static final String UTF8 = "UTF-8";
	
	public static final String TIMESTAMP_FIELD = "lmtsf";
	public static final String STORED_META_FIELD = "lmsmf";
	public static final String STORED_DOC_FIELD = "lmsdf";

}
