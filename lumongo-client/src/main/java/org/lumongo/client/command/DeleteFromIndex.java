package org.lumongo.client.command;


/**
 * Deletes a document from the LuMongo index specified without
 * removing associated documents
 * @author mdavis
 *
 */
public class DeleteFromIndex extends Delete {

	public DeleteFromIndex(String uniqueId, String indexName) {
		super(uniqueId, indexName);
		setDeleteDocument(true);
		setDeleteAllAssociated(false);
	}


}
