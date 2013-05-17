package org.lumongo.client.result;

import java.util.List;

import org.lumongo.cluster.message.Lumongo.GetMembersResponse;
import org.lumongo.cluster.message.Lumongo.IndexMapping;
import org.lumongo.cluster.message.Lumongo.LMMember;

public class GetMembersResult extends Result {

	private GetMembersResponse getMembersResponse;

	public GetMembersResult(GetMembersResponse getMembersResponse) {
		this.getMembersResponse = getMembersResponse;
	}

	public List<LMMember> getMembers() {
		return getMembersResponse.getMemberList();
	}

	public List<IndexMapping> getIndexMappings() {
		return getMembersResponse.getIndexMappingList();
	}

	@Override
	public String toString() {
		return getMembersResponse.toString();
	}

}
