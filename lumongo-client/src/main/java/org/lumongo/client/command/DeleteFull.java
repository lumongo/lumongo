package org.lumongo.client.command;

public class DeleteFull extends Delete {

	public DeleteFull(String uniqueId, String indexName) {
		super(uniqueId, indexName);
		setDeleteDocument(true);
		setDeleteAllAssociated(true);
	}

}
