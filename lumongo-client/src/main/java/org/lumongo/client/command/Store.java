package org.lumongo.client.command;

import java.util.ArrayList;
import java.util.List;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.StoreResult;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.LMDoc;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.cluster.message.Lumongo.StoreRequest;
import org.lumongo.cluster.message.Lumongo.StoreResponse;
import org.lumongo.doc.ResultDocBuilder;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class Store extends SimpleCommand<StoreRequest, StoreResult> {
    private String uniqueId;
    private ResultDocument resultDocument;
    private List<LMDoc> indexedDocuments;
    private List<AssociatedDocument> associatedDocuments;
    private Boolean clearExistingAssociated;

    public Store(String uniqueId) {
        this.uniqueId = uniqueId;
        this.indexedDocuments = new ArrayList<LMDoc>();
        this.associatedDocuments = new ArrayList<AssociatedDocument>();
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public Store setUniqueId(String uniqueId) {
        this.uniqueId = uniqueId;
        return this;
    }

    public ResultDocument getResultDocument() {
        return resultDocument;
    }

    public Store setResultDocument(ResultDocBuilder resultDocumentBuilder) {
    	resultDocumentBuilder.setUniqueId(uniqueId);
        this.resultDocument = resultDocumentBuilder.build();
        return this;
    }

    public Store addIndexedDocument(LMDoc indexedDocument) {
        indexedDocuments.add(indexedDocument);
        return this;
    }

    public List<LMDoc> getIndexedDocuments() {
        return indexedDocuments;
    }

    public Store setIndexedDocuments(List<LMDoc> indexedDocuments) {
        this.indexedDocuments = indexedDocuments;
        return this;
    }

    public Store addAssociatedDocument(AssociatedDocument associatedDocument) {
        associatedDocuments.add(associatedDocument);
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

        if (indexedDocuments != null) {
            storeRequestBuilder.addAllIndexedDocument(indexedDocuments);
        }
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
