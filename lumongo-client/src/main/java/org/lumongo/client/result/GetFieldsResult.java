package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.GetFieldNamesResponse;

import java.util.List;

public class GetFieldsResult extends Result {

	private GetFieldNamesResponse getFieldNamesResponse;

	public GetFieldsResult(GetFieldNamesResponse getFieldNamesResponse) {
		this.getFieldNamesResponse = getFieldNamesResponse;
	}

	public List<String> getFieldNames() {
		return getFieldNamesResponse.getFieldNameList();
	}

	public boolean containsField(String fieldName) {
		return getFieldNamesResponse.getFieldNameList().contains(fieldName);
	}

	@Override
	public String toString() {
		return "GetFieldsResult [fieldNames=" + getFieldNames() + ", commandTimeMs=" + getCommandTimeMs() + "]";
	}

}
