package org.lumongo.client;

import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.lumongo.client.config.LumongoClientConfig;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.ClearRequest;
import org.lumongo.cluster.message.Lumongo.ClearResponse;
import org.lumongo.cluster.message.Lumongo.DeleteRequest;
import org.lumongo.cluster.message.Lumongo.DeleteResponse;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.ExternalService.BlockingInterface;
import org.lumongo.cluster.message.Lumongo.FetchRequest;
import org.lumongo.cluster.message.Lumongo.FetchRequest.FetchType;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesRequest;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;
import org.lumongo.cluster.message.Lumongo.GetIndexesRequest;
import org.lumongo.cluster.message.Lumongo.GetIndexesResponse;
import org.lumongo.cluster.message.Lumongo.GetMembersRequest;
import org.lumongo.cluster.message.Lumongo.GetMembersResponse;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsRequest;
import org.lumongo.cluster.message.Lumongo.GetNumberOfDocsResponse;
import org.lumongo.cluster.message.Lumongo.GetTermsRequest;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;
import org.lumongo.cluster.message.Lumongo.IndexCreateRequest;
import org.lumongo.cluster.message.Lumongo.IndexCreateResponse;
import org.lumongo.cluster.message.Lumongo.IndexDeleteRequest;
import org.lumongo.cluster.message.Lumongo.IndexDeleteResponse;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.cluster.message.Lumongo.IndexSettingsRequest;
import org.lumongo.cluster.message.Lumongo.IndexSettingsResponse;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.LMMember;
import org.lumongo.cluster.message.Lumongo.OptimizeRequest;
import org.lumongo.cluster.message.Lumongo.OptimizeResponse;
import org.lumongo.cluster.message.Lumongo.QueryRequest;
import org.lumongo.cluster.message.Lumongo.QueryResponse;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.cluster.message.Lumongo.StoreRequest;
import org.lumongo.cluster.message.Lumongo.StoreResponse;
import org.lumongo.util.LumongoThreadFactory;

import com.google.protobuf.RpcController;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClient;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientBootstrap;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;

/**
 * Lumongo client
 * 
 * @author mdavis
 * 
 */
public class LumongoClient {
	
	private static LumongoThreadFactory rpcFactory = new LumongoThreadFactory(LumongoClient.class.getSimpleName() + "-Rpc");
	private static LumongoThreadFactory bossFactory = new LumongoThreadFactory(LumongoClient.class.getSimpleName() + "-Boss");
	private static LumongoThreadFactory workerFactory = new LumongoThreadFactory(LumongoClient.class.getSimpleName() + "-Worker");
	
	private List<LMMember> members;
	private int myServerIndex;
	private ExternalService.BlockingInterface service;
	private RpcClient rpcClient;
	private DuplexTcpClientBootstrap bootstrap;
	private final String myHostName;
	private int retryCount;
	
	private static CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
	
	/**
	 * 
	 * @param lumongoClientConfig
	 *            - Gives settings for the client
	 * @throws IOException
	 *             -if cannot get hostname
	 */
	public LumongoClient(LumongoClientConfig lumongoClientConfig) throws IOException {
		
		this.myHostName = InetAddress.getLocalHost().getCanonicalHostName();
		this.retryCount = lumongoClientConfig.getDefaultRetries();
		
		updateMembers(lumongoClientConfig.getMembers());
		
	}
	
	public void openConnection() throws Exception {
		openConnection(retryCount);
	}
	
	protected void openConnection(int retries) throws Exception {
		
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			
			//force something to happen
			getCurrentMembers();
			
		}
		catch (Exception e) {
			
			System.err.println("ERROR: Open connection failed on server <" + members.get(myServerIndex).getServerAddress() + ">: " + e);
			cycleServers();
			
			if (retries > 0) {
				openConnection(retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public void updateMembers(List<LMMember> members) {
		if (members == null || members.isEmpty()) {
			throw new IllegalArgumentException("At least one member must be given");
		}
		
		close();
		
		this.members = members;
		this.myServerIndex = (int) (Math.random() * members.size());
	}
	
	public void setRetryCount(int retriesCount) {
		if (retriesCount < 0) {
			throw new IllegalArgumentException("retries must be postive");
		}
		this.retryCount = retriesCount;
	}
	
	/**
	 * invalidates the current service and cycles to the next server index
	 */
	protected void cycleServers() {
		close();
		myServerIndex = (myServerIndex + 1) % members.size();
	}
	
	public StoreResponse storeDocument(ResultDocument resultDoc, LMDoc indexedDoc) throws Exception {
		StoreRequest.Builder storeRequestBuilder = StoreRequest.newBuilder();
		storeRequestBuilder.addIndexedDocument(indexedDoc);
		storeRequestBuilder.setResultDocument(resultDoc);
		String uniqueId = resultDoc.getUniqueId();
		storeRequestBuilder.setUniqueId(uniqueId);
		return store(storeRequestBuilder.build());
	}
	
	public StoreResponse storeAssociatedDocument(AssociatedDocument associatedDocument) throws Exception {
		StoreRequest.Builder storeRequestBuilder = StoreRequest.newBuilder();
		storeRequestBuilder.addAssociatedDocument(associatedDocument);
		String uniqueId = associatedDocument.getDocumentUniqueId();
		storeRequestBuilder.setUniqueId(uniqueId);
		return store(storeRequestBuilder.build());
	}
	
	public StoreResponse storeDocumentWithAssociated(ResultDocument resultDoc, LMDoc indexedDoc, List<AssociatedDocument> associatedDocuments) throws Exception {
		String uniqueId = resultDoc.getUniqueId();
		
		StoreRequest.Builder storeRequestBuilder = StoreRequest.newBuilder();
		for (AssociatedDocument associatedDocument : associatedDocuments) {
			if (!uniqueId.equals(associatedDocument.getDocumentUniqueId())) {
				throw new IllegalArgumentException("Associate document unique id must match the indexed document's unique id for <" + uniqueId + ">");
			}
		}
		storeRequestBuilder.setUniqueId(uniqueId);
		storeRequestBuilder.addIndexedDocument(indexedDoc);
		storeRequestBuilder.setResultDocument(resultDoc);
		storeRequestBuilder.addAllAssociatedDocument(associatedDocuments);
		return store(storeRequestBuilder.build());
		
	}
	
	protected StoreResponse store(StoreRequest request) throws Exception {
		return store(request, retryCount);
	}
	
	protected StoreResponse store(StoreRequest request, int retries) throws Exception {
		
		RpcController controller = null;
		
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.store(controller, request);
		}
		catch (Exception e) {
			
			System.err.println("ERROR: Index <" + request.getUniqueId() + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			cycleServers();
			
			if (retries > 0) {
				return store(request, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public QueryResponse query(String query, int amount, String index) throws Exception {
		return query(query, amount, index, null);
	}
	
	public QueryResponse query(String query, int amount, String index, QueryResponse lastResponse) throws Exception {
		return query(query, amount, index, lastResponse, retryCount);
	}
	
	protected QueryResponse query(String query, int amount, String index, QueryResponse lastResponse, int retries) throws Exception {
		
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			QueryRequest.Builder requestBuilder = QueryRequest.newBuilder().setQuery(query).setAmount(amount);
			if (lastResponse != null) {
				requestBuilder.setLastResult(lastResponse.getLastResult());
			}
			
			requestBuilder.addIndexes(index);
			
			QueryResponse queryResponse = service.query(controller, requestBuilder.build());
			return queryResponse;
		}
		catch (Exception e) {
			
			System.err.println("ERROR: Query <" + query + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return query(query, amount, index, lastResponse, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public DeleteResponse delete(String uniqueId) throws Exception {
		return delete(uniqueId, retryCount);
	}
	
	protected DeleteResponse delete(String uniqueId, int retries) throws Exception {
		
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			
			controller = rpcClient.newRpcController();
			
			DeleteRequest.Builder requestBuilder = DeleteRequest.newBuilder();
			requestBuilder.setUniqueId(uniqueId);
			requestBuilder.setDeleteDocument(true);
			requestBuilder.setDeleteAllAssociated(true);
			return service.delete(controller, requestBuilder.build());
		}
		catch (Exception e) {
			System.err.println("ERROR: Delete <" + uniqueId + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return delete(uniqueId, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public DeleteResponse deleteAssociated(String uniqueId, String fileName) throws Exception {
		return deleteAssociated(uniqueId, fileName, retryCount);
	}
	
	protected DeleteResponse deleteAssociated(String uniqueId, String fileName, int retries) throws Exception {
		
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			
			controller = rpcClient.newRpcController();
			
			DeleteRequest.Builder requestBuilder = DeleteRequest.newBuilder();
			requestBuilder.setUniqueId(uniqueId);
			requestBuilder.setFilename(fileName);
			requestBuilder.setDeleteDocument(false);
			requestBuilder.setDeleteAllAssociated(false);
			return service.delete(controller, requestBuilder.build());
		}
		catch (Exception e) {
			System.err.println("ERROR: Delete <" + uniqueId + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return delete(uniqueId, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public DeleteResponse deleteAllAssociated(String uniqueId) throws Exception {
		return deleteAllAssociated(uniqueId, retryCount);
	}
	
	protected DeleteResponse deleteAllAssociated(String uniqueId, int retries) throws Exception {
		
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			
			controller = rpcClient.newRpcController();
			
			DeleteRequest.Builder requestBuilder = DeleteRequest.newBuilder();
			requestBuilder.setUniqueId(uniqueId);
			requestBuilder.setDeleteDocument(false);
			requestBuilder.setDeleteAllAssociated(true);
			return service.delete(controller, requestBuilder.build());
		}
		catch (Exception e) {
			System.err.println("ERROR: Delete <" + uniqueId + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return delete(uniqueId, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public FetchResponse fetchDocument(String uniqueId) throws Exception {
		FetchRequest.Builder fetchRequestBuilder = FetchRequest.newBuilder();
		fetchRequestBuilder.setUniqueId(uniqueId);
		fetchRequestBuilder.setResultFetchType(FetchType.FULL);
		fetchRequestBuilder.setAssociatedFetchType(FetchType.NONE);
		return fetch(fetchRequestBuilder.build());
	}
	
	public FetchResponse fetchDocumentAndAssociatedMeta(String uniqueId) throws Exception {
		FetchRequest.Builder fetchRequestBuilder = FetchRequest.newBuilder();
		fetchRequestBuilder.setUniqueId(uniqueId);
		fetchRequestBuilder.setResultFetchType(FetchType.FULL);
		fetchRequestBuilder.setAssociatedFetchType(FetchType.META);
		return fetch(fetchRequestBuilder.build());
	}
	
	public FetchResponse fetchDocumentAndAssociated(String uniqueId) throws Exception {
		FetchRequest.Builder fetchRequestBuilder = FetchRequest.newBuilder();
		fetchRequestBuilder.setUniqueId(uniqueId);
		fetchRequestBuilder.setResultFetchType(FetchType.FULL);
		fetchRequestBuilder.setAssociatedFetchType(FetchType.FULL);
		return fetch(fetchRequestBuilder.build());
	}
	
	public FetchResponse fetchAssociatedDocument(String uniqueId, String fileName) throws Exception {
		FetchRequest.Builder fetchRequestBuilder = FetchRequest.newBuilder();
		fetchRequestBuilder.setUniqueId(uniqueId);
		fetchRequestBuilder.setFileName(fileName);
		fetchRequestBuilder.setResultFetchType(FetchType.NONE);
		fetchRequestBuilder.setAssociatedFetchType(FetchType.FULL);
		return fetch(fetchRequestBuilder.build());
	}
	
	public FetchResponse fetch(FetchRequest request) throws Exception {
		return fetch(request, retryCount);
	}
	
	protected FetchResponse fetch(FetchRequest request, int retries) throws Exception {
		
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.fetch(controller, request);
		}
		catch (Exception e) {
			System.err.println("ERROR: Fetch <" + request + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return fetch(request, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public IndexCreateResponse createIndex(String indexName, int numberOfSegments, String uniqueIdField, IndexSettings indexSettings) throws Exception {
		IndexCreateRequest.Builder indexCreateRequest = IndexCreateRequest.newBuilder();
		indexCreateRequest.setIndexName(indexName);
		indexCreateRequest.setNumberOfSegments(numberOfSegments);
		indexCreateRequest.setUniqueIdField(uniqueIdField);
		indexCreateRequest.setIndexSettings(indexSettings);
		return createIndex(indexCreateRequest.build());
	}
	
	public IndexCreateResponse createIndex(IndexCreateRequest request) throws Exception {
		return createIndex(request, retryCount);
	}
	
	protected IndexCreateResponse createIndex(IndexCreateRequest request, int retries) throws Exception {
		
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.createIndex(controller, request);
		}
		catch (Exception e) {
			System.err.println("ERROR: Create index <" + request + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return createIndex(request, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public IndexSettingsResponse updateIndexSettings(String indexName, IndexSettings indexSettings) throws Exception {
		IndexSettingsRequest isr = IndexSettingsRequest.newBuilder().setIndexName(indexName).setIndexSettings(indexSettings).build();
		return updateIndexSettings(isr, retryCount);
	}
	
	protected IndexSettingsResponse updateIndexSettings(IndexSettingsRequest request, int retries) throws Exception {
		
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.changeIndex(controller, request);
		}
		catch (Exception e) {
			System.err.println("ERROR: Update index <" + request + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return updateIndexSettings(request, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public IndexDeleteResponse deleteIndex(IndexDeleteRequest request) throws Exception {
		return deleteIndex(request, retryCount);
	}
	
	protected IndexDeleteResponse deleteIndex(IndexDeleteRequest request, int retries) throws Exception {
		if (retries < 0) {
			throw new IllegalArgumentException("retries must be postive");
		}
		
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.deleteIndex(controller, request);
		}
		catch (Exception e) {
			System.err.println("ERROR: Delete index <" + request + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return deleteIndex(request, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public GetIndexesResponse getIndexes() throws Exception {
		return getIndexes(retryCount);
	}
	
	protected GetIndexesResponse getIndexes(int retries) throws Exception {
		
		GetIndexesRequest request = GetIndexesRequest.newBuilder().build();
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.getIndexes(controller, request);
		}
		catch (Exception e) {
			System.err.println("ERROR: Get index request <" + request + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return getIndexes(retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public OptimizeResponse optimizeIndex(String indexName) throws Exception {
		return optimizeIndex(indexName, retryCount);
	}
	
	protected OptimizeResponse optimizeIndex(String indexName, int retries) throws Exception {
		
		OptimizeRequest request = OptimizeRequest.newBuilder().setIndexName(indexName).build();
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.optimize(controller, request);
		}
		catch (Exception e) {
			System.err.println("ERROR: Optimize request <" + request + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return optimizeIndex(indexName, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public ClearResponse clearIndex(String indexName) throws Exception {
		return clearIndex(indexName, retryCount);
	}
	
	protected ClearResponse clearIndex(String indexName, int retries) throws Exception {
		
		ClearRequest request = ClearRequest.newBuilder().setIndexName(indexName).build();
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.clear(controller, request);
		}
		catch (Exception e) {
			System.err.println("ERROR: Clear request <" + request + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return clearIndex(indexName, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public GetNumberOfDocsResponse getNumberOfDocs(String indexName) throws Exception {
		return getNumberOfDocs(indexName, retryCount);
	}
	
	public GetNumberOfDocsResponse getNumberOfDocs(String indexName, int retries) throws Exception {
		
		GetNumberOfDocsRequest request = GetNumberOfDocsRequest.newBuilder().setIndexName(indexName).build();
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.getNumberOfDocs(controller, request);
		}
		catch (Exception e) {
			System.err.println("ERROR: Get number of docs request <" + request + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return getNumberOfDocs(indexName, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public GetFieldNamesResponse getFieldNames(String indexName) throws Exception {
		return getFieldNames(indexName, retryCount);
	}
	
	public GetFieldNamesResponse getFieldNames(String indexName, int retries) throws Exception {
		
		GetFieldNamesRequest request = GetFieldNamesRequest.newBuilder().setIndexName(indexName).build();
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.getFieldNames(controller, request);
		}
		catch (Exception e) {
			System.err.println("ERROR: Get field names request <" + request + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return getFieldNames(indexName, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	public GetTermsResponse getTerms(String indexName, String fieldName) throws Exception {
		GetTermsResponse.Builder fullResponse = GetTermsResponse.newBuilder();
		
		Set<String> terms = new LinkedHashSet<String>();
		GetTermsResponse response = null;
		String startTerm = null;
		do {
			response = getTerms(indexName, fieldName, startTerm, 1024 * 64);
			terms.addAll(response.getValueList());
			if (response.getValueCount() > 1) {
				startTerm = response.getValue(response.getValueCount() - 1);
			}
		}
		while ((response != null) && (response.getValueCount() > 1));
		
		fullResponse.addAllValue(terms);
		
		return fullResponse.build();
	}
	
	public GetTermsResponse getTerms(String indexName, String fieldName, int amount) throws Exception {
		return getTerms(indexName, fieldName, null, amount);
	}
	
	public GetTermsResponse getTerms(String indexName, String fieldName, String startTerm, int amount) throws Exception {
		return getTerms(indexName, fieldName, startTerm, amount, retryCount);
	}
	
	protected GetTermsResponse getTerms(String indexName, String fieldName, String startTerm, int amount, int retries) throws Exception {
		
		GetTermsRequest.Builder requestBuilder = GetTermsRequest.newBuilder();
		requestBuilder.setIndexName(indexName);
		requestBuilder.setFieldName(fieldName);
		requestBuilder.setAmount(amount);
		
		if (startTerm != null) {
			requestBuilder.setStartingTerm(startTerm);
		}
		
		GetTermsRequest request = requestBuilder.build();
		
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.getTerms(controller, request);
		}
		catch (Exception e) {
			System.err.println("ERROR: Get terms request <" + request + "> failed on server <" + members.get(myServerIndex).getServerAddress() + ">: "
					+ (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return getTerms(indexName, fieldName, startTerm, retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	/**
	 * Returns a connection to a server node based on the current server index
	 * 
	 * @return
	 * @throws IOException
	 */
	protected ExternalService.BlockingInterface getInternalBlockingConnection() throws IOException {
		LMMember member = members.get(myServerIndex);
		
		PeerInfo server = new PeerInfo(member.getServerAddress(), member.getExternalPort());
		
		PeerInfo client = new PeerInfo(myHostName + "-" + UUID.randomUUID().toString(), 1234);
		
		System.err.println("INFO: Connecting from <" + client + "> to <" + server + ">");
		
		ChannelFactory cf = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(bossFactory), Executors.newCachedThreadPool(workerFactory));
		// should never be used here because we are just doing server side calls
		ThreadPoolCallExecutor executor = new ThreadPoolCallExecutor(1, 1, rpcFactory);
		bootstrap = new DuplexTcpClientBootstrap(client, cf, executor);
		
		bootstrap.setCompression(true);
		
		bootstrap.setRpcLogger(null);
		
		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("connectResponseTimeoutMillis", 10000);
		bootstrap.setOption("receiveBufferSize", 1048576);
		bootstrap.setOption("tcpNoDelay", false);
		shutdownHandler.addResource(bootstrap);
		
		rpcClient = bootstrap.peerWith(server);
		
		BlockingInterface externalService = ExternalService.newBlockingStub(rpcClient);
		
		return externalService;
	}
	
	public GetMembersResponse getCurrentMembers() throws Exception {
		return getCurrentMembers(retryCount);
	}
	
	protected GetMembersResponse getCurrentMembers(int retries) throws Exception {
		GetMembersRequest request = GetMembersRequest.newBuilder().build();
		
		RpcController controller = null;
		try {
			if (service == null) {
				service = getInternalBlockingConnection();
			}
			controller = rpcClient.newRpcController();
			
			return service.getMembers(controller, request);
		}
		catch (Exception e) {
			System.err.println("ERROR: Get current members request <" + request + "> failed on server <" + members.get(myServerIndex).getServerAddress()
					+ ">: " + (controller != null ? controller.errorText() : e.toString()));
			
			cycleServers();
			
			if (retries > 0) {
				return getCurrentMembers(retries - 1);
			}
			else {
				throw new Exception(e.getMessage());
			}
		}
	}
	
	/**
	 * closes the connection to the server if open, calling a method (index, query, ...) will open a new connection
	 */
	public void close() {
		try {
			
			if (rpcClient != null) {
				System.err.println("INFO: Closing connection to " + rpcClient.getPeerInfo());
				rpcClient.close();
			}
		}
		catch (Exception e) {
			System.err.println("ERROR: Exception: " + e);
			e.printStackTrace();
		}
		rpcClient = null;
		try {
			if (bootstrap != null) {
				bootstrap.releaseExternalResources();
			}
		}
		catch (Exception e) {
			System.err.println("ERROR: Exception: " + e);
			e.printStackTrace();
		}
		bootstrap = null;
		service = null;
	}
}
