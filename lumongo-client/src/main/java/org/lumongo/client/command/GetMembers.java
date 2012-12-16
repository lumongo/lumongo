package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.GetMembersResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.GetMembersRequest;
import org.lumongo.cluster.message.Lumongo.GetMembersResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Returns the current cluster members list
 * @author mdavis
 *
 */
public class GetMembers extends SimpleCommand<GetMembersRequest, GetMembersResult> {


    public GetMembers() {

    }

    @Override
    public GetMembersRequest getRequest() {
        return GetMembersRequest.newBuilder().build();
    }

    @Override
    public GetMembersResult execute(LumongoConnection lumongoConnection) throws ServiceException {
        ExternalService.BlockingInterface service = lumongoConnection.getService();

        RpcController controller = lumongoConnection.getController();

        GetMembersResponse getMembersResponse = service.getMembers(controller, getRequest());

        return new GetMembersResult(getMembersResponse);
    }



}
