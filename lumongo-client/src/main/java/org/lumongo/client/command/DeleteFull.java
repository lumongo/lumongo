package org.lumongo.client.command;

public class DeleteFull extends Delete {

    public DeleteFull(String uniqueId) {
        super(uniqueId);
        setDeleteDocument(true);
        setDeleteAllAssociated(true);
    }

}
