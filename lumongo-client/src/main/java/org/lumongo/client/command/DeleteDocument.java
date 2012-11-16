package org.lumongo.client.command;

public class DeleteDocument extends Delete {

    public DeleteDocument(String uniqueId) {
        super(uniqueId);
        setDeleteDocument(true);
        setDeleteAllAssociated(false);
    }

}
