package org.lumongo.client.command;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.bson.Document;
import org.lumongo.client.command.base.RoutableCommand;
import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.StoreResult;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.cluster.message.Lumongo.StoreRequest;
import org.lumongo.cluster.message.Lumongo.StoreResponse;
import org.lumongo.doc.AssociatedBuilder;
import org.lumongo.doc.ResultDocBuilder;

import java.util.ArrayList;
import java.util.List;

public class Store extends SimpleCommand<StoreRequest, StoreResult> implements RoutableCommand {
	private String uniqueId;
	private String indexName;
	private ResultDocument resultDocument;
	
	private List<AssociatedDocument> associatedDocuments;
	private Boolean clearExistingAssociated;
	
	public Store(String uniqueId, String indexName) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
		this.associatedDocuments = new ArrayList<AssociatedDocument>();
	}
	
	@Override
	public String getUniqueId() {
		return uniqueId;
	}
	
	@Override
	public String getIndexName() {
		return indexName;
	}
	
	public Store setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
		return this;
	}
	
	public ResultDocument getResultDocument() {
		return resultDocument;
	}

	public Store setResultDocument(Document document) {
		return setResultDocument(ResultDocBuilder.newBuilder().setDocument(document));
	}

	public Store setResultDocument(ResultDocBuilder resultDocumentBuilder) {
		resultDocumentBuilder.setUniqueId(uniqueId);
		resultDocumentBuilder.setIndexName(indexName);
		this.resultDocument = resultDocumentBuilder.getResultDocument();
		return this;
	}
	
	public Store addAssociatedDocument(AssociatedBuilder associatedBuilder) {
		associatedBuilder.setDocumentUniqueId(uniqueId);
		associatedBuilder.setIndexName(indexName);
		associatedDocuments.add(associatedBuilder.getAssociatedDocument());
		return this;
	}
	
	public List<AssociatedDocument> getAssociatedDocuments() {
		return associatedDocuments;
	}
	
	public Store setAssociatedDocuments(List<AssociatedDocument> associatedDocuments) {
		this.associatedDocuments = associatedDocuments;
		return this;
	}
	
	public Boolean isClearExistingAssociated() {
		return clearExistingAssociated;
	}
	
	public Store setClearExistingAssociated(Boolean clearExistingAssociated) {
		this.clearExistingAssociated = clearExistingAssociated;
		return this;
	}
	
	@Override
	public StoreRequest getRequest() {
		StoreRequest.Builder storeRequestBuilder = StoreRequest.newBuilder();
		storeRequestBuilder.setUniqueId(uniqueId);
		storeRequestBuilder.setIndexName(indexName);
		
		if (resultDocument != null) {
			storeRequestBuilder.setResultDocument(resultDocument);
		}
		if (associatedDocuments != null) {
			storeRequestBuilder.addAllAssociatedDocument(associatedDocuments);
		}
		
		if (clearExistingAssociated != null) {
			storeRequestBuilder.setClearExistingAssociated(clearExistingAssociated);
		}
		return storeRequestBuilder.build();
	}
	
	@Override
	public StoreResult execute(LumongoConnection lumongoConnection) throws ServiceException {
		ExternalService.BlockingInterface service = lumongoConnection.getService();
		
		RpcController controller = lumongoConnection.getController();
		
		StoreResponse storeResponse = service.store(controller, getRequest());
		
		return new StoreResult(storeResponse);
	}
	
}
