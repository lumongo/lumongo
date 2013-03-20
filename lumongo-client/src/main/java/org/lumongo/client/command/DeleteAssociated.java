package org.lumongo.client.command;

/**
 * Removes the associated documents for a unique id with a specific filename
 * @author mdavis
 *
 */
public class DeleteAssociated extends Delete {


	public DeleteAssociated(String uniqueId, String indexName, String fileName) {
		super(uniqueId, indexName);
		setDeleteDocument(false);
		setFileName(fileName);
		setDeleteDocument(false);
		setDeleteAllAssociated(false);
	}

}
