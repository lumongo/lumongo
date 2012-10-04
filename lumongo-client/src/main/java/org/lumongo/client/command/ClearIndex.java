package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.ClearResult;
import org.lumongo.cluster.message.Lumongo.ClearRequest;
import org.lumongo.cluster.message.Lumongo.ClearResponse;
import org.lumongo.cluster.message.Lumongo.ExternalService;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class ClearIndex extends Command<ClearRequest, ClearResult> {

    private String indexName;

    public ClearIndex(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public ClearRequest getRequest() {
        return ClearRequest.newBuilder().setIndexName(indexName).build();
    }

    @Override
    public ClearResult execute(LumongoConnection lumongoConnection) throws ServiceException {
        ExternalService.BlockingInterface service = lumongoConnection.getService();

        RpcController controller = lumongoConnection.getController();

        long start = System.currentTimeMillis();
        ClearResponse clearResponse = service.clear(controller, getRequest());
        long end = System.currentTimeMillis();
        long durationInMs = end - start;
        return new ClearResult(clearResponse, durationInMs);
    }



}
