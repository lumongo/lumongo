package org.lumongo.client.command;


/**
 *  Removes all associated documents for the unique id given
 * @author mdavis
 *
 */
public class DeleteAllAssociated extends Delete {

    public DeleteAllAssociated(String uniqueId) {
        super(uniqueId);
        setDeleteDocument(false);
        setDeleteAllAssociated(true);
    }

}
