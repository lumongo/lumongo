package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.UpdateIndexResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.IndexSettingsRequest;
import org.lumongo.cluster.message.Lumongo.IndexSettingsResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class UpdateIndex extends SimpleCommand<IndexSettingsRequest, UpdateIndexResult> {


    private IndexConfig indexConfig;
    private String indexName;

    public UpdateIndex(String indexName, IndexConfig indexConfig) {
        this.indexConfig = indexConfig;
        this.indexName = indexName;
    }

    @Override
    public IndexSettingsRequest getRequest() {
        IndexSettingsRequest.Builder indexSettingsRequestBuilder = IndexSettingsRequest.newBuilder();
        indexSettingsRequestBuilder.setIndexName(indexName);
        if (indexConfig != null) {
            indexSettingsRequestBuilder.setIndexSettings(indexConfig.getIndexSettings());
        }

        return indexSettingsRequestBuilder.build();
    }


    @Override
    public UpdateIndexResult execute(LumongoConnection lumongoConnection) throws ServiceException {
        ExternalService.BlockingInterface service = lumongoConnection.getService();

        RpcController controller = lumongoConnection.getController();

        IndexSettingsResponse indexSettingsResponse = service.changeIndex(controller, getRequest());

        return new UpdateIndexResult(indexSettingsResponse);
    }




}
