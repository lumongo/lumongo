package org.lumongo.client.command;

import org.lumongo.client.command.base.RoutableCommand;
import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.FetchResult;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo.FetchRequest;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.FetchType;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class Fetch extends SimpleCommand<FetchRequest, FetchResult> implements RoutableCommand {

	private String uniqueId;
	private String indexName;
	private String filename;
	private FetchType resultFetchType;
	private FetchType associatedFetchType;

	private Set<String> documentFields = Collections.emptySet();
	private Set<String> documentMaskedFields = Collections.emptySet();

	private Long timestamp;

	public Fetch(String uniqueId, String indexName) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
	}

	public String getUniqueId() {
		return uniqueId;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	public Fetch setFilename(String filename) {
		this.filename = filename;
		return this;
	}

	public String getFileFame() {
		return filename;
	}

	public FetchType getResultFetchType() {
		return resultFetchType;
	}

	public Fetch setResultFetchType(FetchType resultFetchType) {
		this.resultFetchType = resultFetchType;
		return this;
	}

	public FetchType getAssociatedFetchType() {
		return associatedFetchType;
	}

	public Fetch setAssociatedFetchType(FetchType associatedFetchType) {
		this.associatedFetchType = associatedFetchType;
		return this;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Set<String> getDocumentMaskedFields() {
		return documentMaskedFields;
	}

	public Fetch addDocumentMaskedField(String documentMaskedField) {
		if (documentMaskedFields.isEmpty()) {
			documentMaskedFields = new LinkedHashSet<>();
		}

		documentMaskedFields.add(documentMaskedField);
		return this;
	}

	public Fetch setDocumentMaskedFields(Collection<String> documentMaskedFields) {
		this.documentMaskedFields = new LinkedHashSet<>(documentMaskedFields);
		return this;
	}

	public Set<String> getDocumentFields() {
		return documentFields;
	}

	public Fetch addDocumentField(String documentField) {
		if (documentFields.isEmpty()) {
			this.documentFields = new LinkedHashSet<>();
		}
		documentFields.add(documentField);
		return this;
	}

	public Fetch setDocumentFields(Collection<String> documentFields) {
		this.documentFields = new LinkedHashSet<>(documentFields);
		return this;
	}

	@Override
	public FetchRequest getRequest() {
		FetchRequest.Builder fetchRequestBuilder = FetchRequest.newBuilder();
		if (uniqueId != null) {
			fetchRequestBuilder.setUniqueId(uniqueId);
		}
		if (indexName != null) {
			fetchRequestBuilder.setIndexName(indexName);
		}
		if (filename != null) {
			fetchRequestBuilder.setFilename(filename);
		}
		if (resultFetchType != null) {
			fetchRequestBuilder.setResultFetchType(resultFetchType);
		}
		if (associatedFetchType != null) {
			fetchRequestBuilder.setAssociatedFetchType(associatedFetchType);
		}
		if (timestamp != null) {
			fetchRequestBuilder.setTimestamp(timestamp);
		}
		fetchRequestBuilder.addAllDocumentFields(documentFields);
		fetchRequestBuilder.addAllDocumentMaskedFields(documentMaskedFields);

		return fetchRequestBuilder.build();
	}

	@Override
	public FetchResult execute(LumongoConnection lumongoConnection) {

		ExternalServiceGrpc.ExternalServiceBlockingStub service = lumongoConnection.getService();

		FetchResponse fetchResponse = service.fetch(getRequest());

		return new FetchResult(fetchResponse);

	}

}
