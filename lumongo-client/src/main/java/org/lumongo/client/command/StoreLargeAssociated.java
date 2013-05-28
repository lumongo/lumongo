package org.lumongo.client.command;

import java.io.File;
import java.io.InputStream;

import org.lumongo.client.LumongoRestClient;
import org.lumongo.client.command.base.RestCommand;
import org.lumongo.client.command.base.RoutableCommand;
import org.lumongo.client.result.StoreLargeAssociatedResult;

public class StoreLargeAssociated extends RestCommand<StoreLargeAssociatedResult> implements RoutableCommand {

	private String uniqueId;
	private String fileName;
	private String indexName;
	private File fileToStore;
	private InputStream source;

	public StoreLargeAssociated(String uniqueId, String indexName, String fileName, File fileToStore) {
		this.uniqueId = uniqueId;
		this.fileName = fileName;
		this.indexName = indexName;
		this.fileToStore = fileToStore;
	}

	public StoreLargeAssociated(String uniqueId, String indexName, String fileName, InputStream source) {
		this.uniqueId = uniqueId;
		this.fileName = fileName;
		this.indexName = indexName;
		this.source = source;
	}

	@Override
	public String getUniqueId() {
		return uniqueId;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	@Override
	public StoreLargeAssociatedResult execute(LumongoRestClient lumongoRestClient) throws Exception {
		if (fileToStore != null) {
			lumongoRestClient.storeAssociated(uniqueId, indexName, fileName, fileToStore);
		}
		else if (source != null) {
			lumongoRestClient.storeAssociated(uniqueId, indexName, fileName, source);
		}
		else {
			throw new Exception("File or input stream must be set");
		}
		return new StoreLargeAssociatedResult();
	}



}
