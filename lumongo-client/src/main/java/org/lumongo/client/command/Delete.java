package org.lumongo.client.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.DeleteResult;
import org.lumongo.cluster.message.Lumongo.DeleteRequest;
import org.lumongo.cluster.message.Lumongo.DeleteResponse;
import org.lumongo.cluster.message.Lumongo.ExternalService;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class Delete extends SimpleCommand<DeleteRequest, DeleteResult> {
    private Collection<String> indexes;
    private String uniqueId;
    private String fileName;
    private Boolean deleteDocument;
    private Boolean deleteAllAssociated;

    public Delete(String uniqueId) {
        this.uniqueId = uniqueId;
    }

    public void setIndexes(Collection<String> indexes) {
        this.indexes = indexes;
    }

    public void setIndexes(String[] indexes) {
        setIndexes(new ArrayList<String>(Arrays.asList(indexes)));
    }

    public void setIndex(String index) {
        setIndexes(new String[] { index });
    }

    public Collection<String> getIndexes() {
        return indexes;
    }

    public Delete setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    public String getFileName() {
        return fileName;
    }

    public Delete setDeleteDocument(Boolean deleteDocument) {
        this.deleteDocument = deleteDocument;
        return this;
    }

    public Boolean getDeleteDocument() {
        return deleteDocument;
    }

    public Delete setDeleteAllAssociated(Boolean deleteAllAssociated) {
        this.deleteAllAssociated = deleteAllAssociated;
        return this;
    }

    public Boolean getDeleteAllAssociated() {
        return deleteAllAssociated;
    }

    @Override
    public DeleteRequest getRequest() {
        DeleteRequest.Builder deleteRequestBuilder = DeleteRequest.newBuilder();
        if (uniqueId != null) {
            deleteRequestBuilder.setUniqueId(uniqueId);
        }
        if (indexes != null) {
            deleteRequestBuilder.addAllIndexes(indexes);
        }
        if (fileName != null) {
            deleteRequestBuilder.setFilename(fileName);
        }
        if (deleteDocument != null) {
            deleteRequestBuilder.setDeleteDocument(deleteDocument);
        }
        if (deleteAllAssociated != null) {
            deleteRequestBuilder.setDeleteAllAssociated(deleteAllAssociated);
        }
        return deleteRequestBuilder.build();
    }

    @Override
    public DeleteResult execute(LumongoConnection lumongoConnection) throws ServiceException {
        ExternalService.BlockingInterface service = lumongoConnection.getService();

        RpcController controller = lumongoConnection.getController();

        DeleteResponse deleteResponse = service.delete(controller, getRequest());

        return new DeleteResult(deleteResponse);
    }

}
