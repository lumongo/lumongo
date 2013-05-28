package org.lumongo.client.command;

import java.io.File;
import java.io.OutputStream;

import org.lumongo.client.LumongoRestClient;
import org.lumongo.client.command.base.RestCommand;
import org.lumongo.client.result.FetchLargeAssociatedResult;

public class FetchLargeAssociated extends RestCommand<FetchLargeAssociatedResult> {

	private String uniqueId;
	private String fileName;
	private File outputFile;
	private OutputStream destination;
	private String indexName;

	public FetchLargeAssociated(String uniqueId, String indexName, String fileName, File outputFile) {
		this.uniqueId = uniqueId;
		this.fileName = fileName;
		this.outputFile = outputFile;
		this.indexName = indexName;
	}

	public FetchLargeAssociated(String uniqueId, String indexName, String fileName, OutputStream destination) {
		this.uniqueId = uniqueId;
		this.fileName = fileName;
		this.destination = destination;
		this.indexName = indexName;
	}

	@Override
	public FetchLargeAssociatedResult execute(LumongoRestClient lumongoRestClient) throws Exception {
		if (outputFile != null) {
			lumongoRestClient.fetchAssociated(uniqueId, indexName, fileName, outputFile);
		}
		else if (destination != null) {
			lumongoRestClient.fetchAssociated(uniqueId, indexName, fileName, destination);
		}
		else {
			throw new Exception("File or output stream must be set");
		}
		return new FetchLargeAssociatedResult();
	}

}
