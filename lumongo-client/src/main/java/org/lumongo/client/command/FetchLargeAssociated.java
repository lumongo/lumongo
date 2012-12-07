package org.lumongo.client.command;

import java.io.File;
import java.io.OutputStream;

import org.lumongo.client.LumongoRestClient;
import org.lumongo.client.result.FetchLargeAssociatedResult;

public class FetchLargeAssociated extends RestCommand<FetchLargeAssociatedResult> {

    private String uniqueId;
    private String fileName;
    private File outputFile;
    private OutputStream destination;

    public FetchLargeAssociated(String uniqueId, String fileName, File outputFile) {
        this.uniqueId = uniqueId;
        this.fileName = fileName;
        this.outputFile = outputFile;
    }

    public FetchLargeAssociated(String uniqueId, String fileName, OutputStream destination) {
        this.uniqueId = uniqueId;
        this.fileName = fileName;
        this.destination = destination;
    }

    @Override
    public FetchLargeAssociatedResult execute(LumongoRestClient lumongoRestClient) throws Exception {
        if (outputFile != null) {
            lumongoRestClient.fetchAssociated(uniqueId, fileName, outputFile);
        }
        else if (destination != null) {
            lumongoRestClient.fetchAssociated(uniqueId, fileName, destination);
        }
        else {
            throw new Exception("File or output stream must be set");
        }
        return new FetchLargeAssociatedResult();
    }

}
