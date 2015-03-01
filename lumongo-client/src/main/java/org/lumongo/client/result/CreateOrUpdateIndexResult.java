package org.lumongo.client.result;

public class CreateOrUpdateIndexResult extends Result {

	private UpdateIndexResult updateIndexResult;
	private CreateIndexResult createIndexResult;

	public CreateOrUpdateIndexResult() {

	}

	public void setUpdateIndexResult(UpdateIndexResult updateIndexResult) {
		this.updateIndexResult = updateIndexResult;
	}

	public void setCreateIndexResult(CreateIndexResult createIndexResult) {
		this.createIndexResult = createIndexResult;
	}

	public boolean isNewIndex() {
		return (createIndexResult != null);
	}

	public boolean isUpdatedIndex() {
		return (updateIndexResult != null);
	}
}
