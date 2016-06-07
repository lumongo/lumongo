package org.lumongo.client.command;

import org.lumongo.client.LumongoRestClient;
import org.lumongo.client.command.base.RestCommand;
import org.lumongo.client.command.base.RoutableCommand;
import org.lumongo.client.result.StoreLargeAssociatedResult;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

public class StoreLargeAssociated extends RestCommand<StoreLargeAssociatedResult> implements RoutableCommand {

	private String uniqueId;
	private String fileName;
	private String indexName;
	private File fileToStore;
	private InputStream source;
	private Boolean compressed;
	private Map<String, String> meta;

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

	public Map<String, String> getMeta() {
		return meta;
	}

	public StoreLargeAssociated setMeta(Map<String, String> meta) {
		this.meta = meta;
		return this;
	}

	public Boolean getCompressed() {
		return compressed;
	}

	public StoreLargeAssociated setCompressed(Boolean compressed) {
		this.compressed = compressed;
		return this;
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
		InputStream input = source;
		if (fileToStore != null) {
			input = new FileInputStream(fileToStore);
		}

		if (input != null) {
			lumongoRestClient.storeAssociated(uniqueId, indexName, fileName, meta, input, compressed);
		}
		else {
			throw new Exception("File or input stream must be set");
		}
		return new StoreLargeAssociatedResult();
	}


}
