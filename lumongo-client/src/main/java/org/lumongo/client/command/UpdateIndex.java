package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.UpdateIndexResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.IndexSettings;
import org.lumongo.cluster.message.Lumongo.IndexSettingsRequest;
import org.lumongo.cluster.message.Lumongo.IndexSettingsResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class UpdateIndex extends Command<IndexSettingsRequest, UpdateIndexResult> {


    private IndexSettings indexSettings;

    public UpdateIndex(IndexSettings indexSettings) {
        this.indexSettings = indexSettings;
    }

    @Override
    public IndexSettingsRequest getRequest() {
        IndexSettingsRequest.Builder indexSettingsRequestBuilder = IndexSettingsRequest.newBuilder();

        if (indexSettings != null) {
            indexSettingsRequestBuilder.setIndexSettings(indexSettings);
        }

        return indexSettingsRequestBuilder.build();
    }


    @Override
    public UpdateIndexResult execute(LumongoConnection lumongoConnection) throws ServiceException {
        ExternalService.BlockingInterface service = lumongoConnection.getService();

        RpcController controller = lumongoConnection.getController();

        long start = System.currentTimeMillis();
        IndexSettingsResponse indexSettingsResponse = service.changeIndex(controller, getRequest());
        long end = System.currentTimeMillis();
        long durationInMs = end - start;
        return new UpdateIndexResult(indexSettingsResponse, durationInMs);
    }




}
