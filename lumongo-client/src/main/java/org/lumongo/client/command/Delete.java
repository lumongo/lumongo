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

    protected void setIndexes(Collection<String> indexes) {
        this.indexes = indexes;
    }

    protected void setIndexes(String[] indexes) {
        setIndexes(new ArrayList<String>(Arrays.asList(indexes)));
    }

    protected void setIndex(String index) {
        setIndexes(new String[] { index });
    }

    protected Collection<String> getIndexes() {
        return indexes;
    }

    protected Delete setFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }

    protected String getFileName() {
        return fileName;
    }

    protected Delete setDeleteDocument(Boolean deleteDocument) {
        this.deleteDocument = deleteDocument;
        return this;
    }

    protected Boolean getDeleteDocument() {
        return deleteDocument;
    }

    protected Delete setDeleteAllAssociated(Boolean deleteAllAssociated) {
        this.deleteAllAssociated = deleteAllAssociated;
        return this;
    }

    protected Boolean getDeleteAllAssociated() {
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
