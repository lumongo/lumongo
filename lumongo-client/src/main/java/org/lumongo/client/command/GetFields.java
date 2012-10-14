package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.GetFieldsResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesRequest;
import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Returns all the fields from a given index
 * @author mdavis
 *
 */
public class GetFields extends SimpleCommand<GetFieldNamesRequest, GetFieldsResult> {

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

        GetFieldNamesResponse getFieldNamesResponse = service.getFieldNames(controller, getRequest());

        return new GetFieldsResult(getFieldNamesResponse);
    }



}
