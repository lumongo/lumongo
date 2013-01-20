package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.CreateIndexResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.IndexCreateRequest;
import org.lumongo.cluster.message.Lumongo.IndexCreateResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Creates a new index with the name, number of segments, unique id field, and IndexSettings given.  Whether the index supports faceting
 * or not is also configurable.  However, only the IndexConfig cannot be changed after the index is created.  If index already exists an exception will be thrown.
 * See @CreateOrUpdateIndex
 * 
 * @author mdavis
 *
 */
public class CreateIndex extends SimpleCommand<IndexCreateRequest, CreateIndexResult> {

    private String indexName;
    private Integer numberOfSegments;
    private String uniqueIdField;
    private Boolean faceted;
    private IndexConfig indexConfig;

    public CreateIndex(String indexName, Integer numberOfSegments, String uniqueIdField, IndexConfig indexConfig) {
        this.indexName = indexName;
        this.numberOfSegments = numberOfSegments;
        this.uniqueIdField = uniqueIdField;
        this.indexConfig = indexConfig;
    }

    public CreateIndex setFaceted(Boolean faceted) {
        this.faceted = faceted;
        return this;
    }

    public Boolean getFaceted() {
        return faceted;
    }

    @Override
    public IndexCreateRequest getRequest() {
        IndexCreateRequest.Builder indexCreateRequestBuilder = IndexCreateRequest.newBuilder();

        if (indexName != null) {
            indexCreateRequestBuilder.setIndexName(indexName);
        }

        if (numberOfSegments != null) {
            indexCreateRequestBuilder.setNumberOfSegments(numberOfSegments);
        }

        if (uniqueIdField != null) {
            indexCreateRequestBuilder.setUniqueIdField(uniqueIdField);
        }

        if (indexConfig != null) {
            indexCreateRequestBuilder.setIndexSettings(indexConfig.getIndexSettings());
        }

        if (faceted != null) {
            indexCreateRequestBuilder.setFaceted(faceted);
        }
        return indexCreateRequestBuilder.build();
    }

    @Override
    public CreateIndexResult execute(LumongoConnection lumongoConnection) throws ServiceException {
        ExternalService.BlockingInterface service = lumongoConnection.getService();

        RpcController controller = lumongoConnection.getController();

        IndexCreateResponse indexCreateResponse = service.createIndex(controller, getRequest());

        return new CreateIndexResult(indexCreateResponse);
    }

}
