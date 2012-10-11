package org.lumongo.client.command;


public class DeleteAllAssociated extends Delete {

    public DeleteAllAssociated(String uniqueId) {
        super(uniqueId);
        setDeleteDocument(false);
        setDeleteAllAssociated(true);
    }

}
