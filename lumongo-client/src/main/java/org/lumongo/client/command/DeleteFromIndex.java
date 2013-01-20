package org.lumongo.client.command;

import java.util.Collection;

/**
 * Deletes a document from the LuMongo index(es) specified without
 * removing the stored document or associated documents
 * @author mdavis
 *
 */
public class DeleteFromIndex extends Delete {

    public DeleteFromIndex(String uniqueId, String index) {
        super(uniqueId);
        setIndex(index);
        setDeleteDocument(false);
        setDeleteAllAssociated(false);
    }

    public DeleteFromIndex(String uniqueId, String[] indexes) {
        super(uniqueId);
        setIndexes(indexes);
        setDeleteDocument(false);
        setDeleteAllAssociated(false);
    }

    public DeleteFromIndex(String uniqueId, Collection<String> indexes) {
        super(uniqueId);
        setIndexes(indexes);
        setDeleteDocument(false);
        setDeleteAllAssociated(false);
    }

}
