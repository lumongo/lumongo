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

	public boolean isNew() {
		return (createIndexResult != null);
	}

	public boolean isUpdate() {
		return (updateIndexResult != null);
	}
}
