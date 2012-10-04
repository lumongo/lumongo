package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.DeleteIndexResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.IndexDeleteRequest;
import org.lumongo.cluster.message.Lumongo.IndexDeleteResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class DeleteIndex extends Command<IndexDeleteRequest, DeleteIndexResult> {

    private String indexName;

    public DeleteIndex(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public IndexDeleteRequest getRequest() {
        return IndexDeleteRequest.newBuilder().setIndexName(indexName).build();
    }

    @Override
    public DeleteIndexResult execute(LumongoConnection lumongoConnection) throws ServiceException {
        ExternalService.BlockingInterface service = lumongoConnection.getService();

        RpcController controller = lumongoConnection.getController();

        long start = System.currentTimeMillis();
        IndexDeleteResponse indexDeleteResponse = service.deleteIndex(controller, getRequest());
        long end = System.currentTimeMillis();
        long durationInMs = end - start;
        return new DeleteIndexResult(indexDeleteResponse, durationInMs);
    }



}
