package org.lumongo.client.command;

public class DeleteDocument extends Delete {

	public DeleteDocument(String uniqueId, String indexName) {
		super(uniqueId, indexName);
		setDeleteDocument(true);
		setDeleteAllAssociated(false);
	}

}
