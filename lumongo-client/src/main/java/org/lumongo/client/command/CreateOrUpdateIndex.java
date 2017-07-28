package org.lumongo.client.command;

import org.lumongo.client.command.base.Command;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.CreateIndexResult;
import org.lumongo.client.result.CreateOrUpdateIndexResult;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.UpdateIndexResult;

/**
 * Creates a new index with all settings given or updates the IndexSettings on an existing index
 * @author mdavis
 *
 */
public class CreateOrUpdateIndex extends Command<CreateOrUpdateIndexResult> {
	private String indexName;
	private Integer numberOfSegments;
	private IndexConfig indexConfig;

	public CreateOrUpdateIndex(String indexName, Integer numberOfSegments, IndexConfig indexConfig) {
		this.indexName = indexName;
		this.numberOfSegments = numberOfSegments;
		this.indexConfig = indexConfig;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public Integer getNumberOfSegments() {
		return numberOfSegments;
	}

	public void setNumberOfSegments(Integer numberOfSegments) {
		this.numberOfSegments = numberOfSegments;
	}

	public IndexConfig getIndexConfig() {
		return indexConfig;
	}

	public void setIndexConfig(IndexConfig indexConfig) {
		this.indexConfig = indexConfig;
	}

	@Override
	public CreateOrUpdateIndexResult execute(LumongoConnection lumongoConnection) {
		CreateOrUpdateIndexResult result = new CreateOrUpdateIndexResult();

		GetIndexes gt = new GetIndexes();
		GetIndexesResult gtr = gt.execute(lumongoConnection);
		if (gtr.containsIndex(indexName)) {
			UpdateIndex ui = new UpdateIndex(indexName, indexConfig);
			UpdateIndexResult uir = ui.execute(lumongoConnection);
			result.setUpdateIndexResult(uir);
			return result;
		}

		CreateIndex ci = new CreateIndex(indexName, numberOfSegments, indexConfig);

		CreateIndexResult cir = ci.execute(lumongoConnection);
		result.setCreateIndexResult(cir);
		return result;

	}

}
