package org.lumongo.client.command;

import java.io.File;
import java.io.InputStream;

import org.lumongo.client.LumongoRestClient;
import org.lumongo.client.result.StoreLargeAssociatedResult;

public class StoreLargeAssociated extends RestCommand<StoreLargeAssociatedResult> {

    private String uniqueId;
    private String fileName;
    private File fileToStore;
    private InputStream source;

    public StoreLargeAssociated(String uniqueId, String fileName, File fileToStore) {
        this.uniqueId = uniqueId;
        this.fileName = fileName;
        this.fileToStore = fileToStore;
    }

    public StoreLargeAssociated(String uniqueId, String fileName, InputStream source) {
        this.uniqueId = uniqueId;
        this.fileName = fileName;
        this.source = source;
    }

    @Override
    public StoreLargeAssociatedResult execute(LumongoRestClient lumongoRestClient) throws Exception {
        if (fileToStore != null) {
            lumongoRestClient.storeAssociated(uniqueId, fileName, fileToStore);
        }
        else if (source != null) {
            lumongoRestClient.storeAssociated(uniqueId, fileName, source);
        }
        else {
            throw new Exception("File or input stream must be set");
        }
        return new StoreLargeAssociatedResult();
    }

}
