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
	public static final int INTERNAL_ERROR = 500;

	public static final String GET = "GET";
	public static final String POST = "POST";

	public static final String ASSOCIATED_DOCUMENTS_URL = "/associatedDocs";
	public static final String QUERY_URL = "query";
	public static final String FETCH_URL = "fetch";
	public static final String FIELDS_URL = "fields";
	public static final String TERMS_URL = "terms";
	public static final String INDEX_URL = "index";
	public static final String INDEXES_URL = "indexes";
	public static final String MEMBERS_URL = "members";
	public static final String STATS_URL = "stats";

	public static final String QUERY = "q";
	public static final String QUERY_FIELD = "qf";
	public static final String FILTER_QUERY = "fq";
	public static final String FILTER_QUERY_JSON = "fqJson";
	public static final String ROWS = "rows";
	public static final String ID = "id";
	public static final String FILE_NAME = "fileName";
	public static final String COMPRESSED = "compressed";
	public static final String META = "meta";
	public static final String INDEX = "index";
	public static final String FACET = "facet";
	public static final String SORT = "sort";
	public static final String FETCH = "fetch";
	public static final String FIELDS = "fl";
	public static final String PRETTY = "pretty";
	public static final String COMPUTE_FACET_ERROR = "computeFacetError";
	public static final String MIN_MATCH = "mm";
	public static final String DEBUG = "debug";
	public static final String AMOUNT = "amount";
	public static final String MIN_DOC_FREQ = "minDocFreq";
	public static final String MIN_TERM_FREQ = "minTermFreq";
	public static final String START_TERM = "startTerm";
	public static final String END_TERM = "endTerm";
	public static final String DEFAULT_OP = "defaultOp";
	public static final String DRILL_DOWN = "drillDown";
	public static final String DISMAX = "dismax";
	public static final String DISMAX_TIE = "dismaxTie";
	public static final String SIMILARITY = "sim";
	public static final String START = "start";

	public static final String TERM_FILTER = "termFilter";
	public static final String TERM_MATCH = "termMatch";
	public static final String INCLUDE_TERM = "includeTerm";

	public static final String FORMAT = "format";

	//General
	public static final String UTF8 = "UTF-8";

	public static final String TIMESTAMP_FIELD = "_lmtsf_";
	public static final String STORED_META_FIELD = "_lmsmf_";
	public static final String STORED_DOC_FIELD = "_lmsdf_";
	public static final String ID_FIELD = "_lmidf_";
	public static final String FIELDS_LIST_FIELD = "_lmflf_";
	public static final String SUPERBIT_PREFIX = "_lmsb_";

	public static final String HIGHLIGHT = "hl";

	public static final String HIGHLIGHT_JSON = "hlJson";

	public static final String ANALYZE_JSON = "alJson";
	public static final String FUZZY_TERM_JSON = "fuzzyTermJson";
	public static final String COS_SIM_JSON = "cosSimJson";

	public static final String DONT_CACHE = "dontCache";
	public static final String BATCH = "batch";
}
