package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.CreateIndexResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.IndexCreateRequest;
import org.lumongo.cluster.message.Lumongo.IndexCreateResponse;
import org.lumongo.cluster.message.Lumongo.IndexSettings;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class CreateIndex extends Command<IndexCreateRequest, CreateIndexResult> {

    private String indexName;
    private Integer numberOfSegments;
    private String uniqueIdField;
    private Boolean faceted;
    private IndexSettings indexSettings;

    public CreateIndex(String indexName, Integer numberOfSegments, String uniqueIdField, IndexSettings indexSettings) {
        this.indexName = indexName;
        this.numberOfSegments = numberOfSegments;
        this.uniqueIdField = uniqueIdField;
        this.indexSettings = indexSettings;
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

        if (indexSettings != null) {
            indexCreateRequestBuilder.setIndexSettings(indexSettings);
        }
        return indexCreateRequestBuilder.build();
    }

    @Override
    public CreateIndexResult execute(LumongoConnection lumongoConnection) throws ServiceException {
        ExternalService.BlockingInterface service = lumongoConnection.getService();

        RpcController controller = lumongoConnection.getController();

        long start = System.currentTimeMillis();
        IndexCreateResponse indexCreateResponse = service.createIndex(controller, getRequest());
        long end = System.currentTimeMillis();
        long durationInMs = end - start;
        return new CreateIndexResult(indexCreateResponse, durationInMs);
    }

}
