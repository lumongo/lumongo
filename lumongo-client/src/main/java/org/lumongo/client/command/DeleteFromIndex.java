package org.lumongo.client.command;


/**
 * Deletes a document from the LuMongo index(es) specified without
 * removing the stored document or associated documents
 * @author mdavis
 *
 */
public class DeleteFromIndex extends Delete {

	public DeleteFromIndex(String uniqueId, String indexName) {
		super(uniqueId, indexName);
		setDeleteDocument(false);
		setDeleteAllAssociated(false);
	}


}
