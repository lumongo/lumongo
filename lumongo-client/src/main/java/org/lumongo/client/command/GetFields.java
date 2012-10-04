package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.GetFieldsResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesRequest;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class GetFields extends Command<GetFieldNamesRequest, GetFieldsResult> {

    private String indexName;

    public GetFields(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public GetFieldNamesRequest getRequest() {
        return GetFieldNamesRequest.newBuilder().setIndexName(indexName).build();
    }

    @Override
    public GetFieldsResult execute(LumongoConnection lumongoConnection) throws ServiceException {
        ExternalService.BlockingInterface service = lumongoConnection.getService();

        RpcController controller = lumongoConnection.getController();

        long start = System.currentTimeMillis();
        GetFieldNamesResponse getFieldNamesResponse = service.getFieldNames(controller, getRequest());
        long end = System.currentTimeMillis();
        long durationInMs = end - start;
        return new GetFieldsResult(getFieldNamesResponse, durationInMs);
    }



}
