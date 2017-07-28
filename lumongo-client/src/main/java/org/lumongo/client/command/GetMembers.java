package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.GetMembersResult;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.GetMembersRequest;
import org.lumongo.cluster.message.Lumongo.GetMembersResponse;

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
	public GetMembersResult execute(LumongoConnection lumongoConnection) {
		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		GetMembersResponse getMembersResponse = service.getMembers(getRequest());

		return new GetMembersResult(getMembersResponse);
	}

}
