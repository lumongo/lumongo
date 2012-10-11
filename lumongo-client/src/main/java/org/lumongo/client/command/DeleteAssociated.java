package org.lumongo.client.command;


public class DeleteAssociated extends Delete {

    public DeleteAssociated(String uniqueId, String fileName) {
        super(uniqueId);
        setDeleteDocument(false);
        setFileName(fileName);
        setDeleteDocument(false);
        setDeleteAllAssociated(false);
    }

}
