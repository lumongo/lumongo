package org.lumongo.client.command;

import org.lumongo.client.command.base.Command;
import org.lumongo.client.config.IndexConfig;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.CreateIndexResult;
import org.lumongo.client.result.CreateOrUpdateIndexResult;
import org.lumongo.client.result.GetIndexesResult;
import org.lumongo.client.result.UpdateIndexResult;

import com.google.protobuf.ServiceException;

/**
 * Creates a new index with all settings given or updates the IndexSettings on an existing index
 * @author mdavis
 *
 */
public class CreateOrUpdateIndex extends Command<CreateOrUpdateIndexResult> {
	private String indexName;
	private Integer numberOfSegments;
	private String uniqueIdField;
	private Boolean faceted;
	private IndexConfig indexConfig;
	private Boolean databasePerRawDocumentSegment;
	private Boolean collectionPerRawDocumentSegment;
	private Boolean databasePerIndexSegment;

	public CreateOrUpdateIndex(String indexName, Integer numberOfSegments, String uniqueIdField, IndexConfig indexConfig) {
		this.indexName = indexName;
		this.numberOfSegments = numberOfSegments;
		this.uniqueIdField = uniqueIdField;
		this.indexConfig = indexConfig;
	}

	public CreateOrUpdateIndex setFaceted(Boolean faceted) {
		this.faceted = faceted;
		return this;
	}

	public Boolean getFaceted() {
		return faceted;
	}

	public Boolean getDatabasePerIndexSegment() {
		return databasePerIndexSegment;
	}

	public void setDatabasePerIndexSegment(Boolean databasePerIndexSegment) {
		this.databasePerIndexSegment = databasePerIndexSegment;
	}

	public Boolean getCollectionPerRawDocumentSegment() {
		return collectionPerRawDocumentSegment;
	}

	public void setCollectionPerRawDocumentSegment(Boolean collectionPerRawDocumentSegment) {
		this.collectionPerRawDocumentSegment = collectionPerRawDocumentSegment;
	}

	public Boolean getDatabasePerRawDocumentSegment() {
		return databasePerRawDocumentSegment;
	}

	public void setDatabasePerRawDocumentSegment(Boolean databasePerRawDocumentSegment) {
		this.databasePerRawDocumentSegment = databasePerRawDocumentSegment;
	}

	@Override
	public CreateOrUpdateIndexResult execute(LumongoConnection lumongoConnection) throws ServiceException {
		CreateOrUpdateIndexResult result = new CreateOrUpdateIndexResult();

		GetIndexes gt = new GetIndexes();
		GetIndexesResult gtr = gt.execute(lumongoConnection);
		if (gtr.containsIndex(indexName)) {
			UpdateIndex ui = new UpdateIndex(indexName, indexConfig);
			UpdateIndexResult uir = ui.execute(lumongoConnection);
			result.setUpdateIndexResult(uir);
			return result;
		}

		CreateIndex ci = new CreateIndex(indexName, numberOfSegments, uniqueIdField, indexConfig);
		ci.setFaceted(faceted);
		ci.setDatabasePerIndexSegment(databasePerIndexSegment);
		ci.setCollectionPerRawDocumentSegment(collectionPerRawDocumentSegment);
		ci.setDatabasePerRawDocumentSegment(databasePerRawDocumentSegment);

		CreateIndexResult cir = ci.execute(lumongoConnection);
		result.setCreateIndexResult(cir);
		return result;

	}







}
