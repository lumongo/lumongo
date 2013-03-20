package org.lumongo.client.command;


/**
 *  Removes all associated documents for the unique id given
 * @author mdavis
 *
 */
public class DeleteAllAssociated extends Delete {

	public DeleteAllAssociated(String uniqueId, String indexName) {
		super(uniqueId, indexName);
		setDeleteDocument(false);
		setDeleteAllAssociated(true);
	}

}
