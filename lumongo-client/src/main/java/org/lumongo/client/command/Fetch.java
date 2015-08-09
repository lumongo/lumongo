package org.lumongo.client.command;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.lumongo.client.command.base.RoutableCommand;
import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.FetchResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.FetchRequest;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.FetchType;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class Fetch extends SimpleCommand<FetchRequest, FetchResult> implements RoutableCommand {

	private String uniqueId;
	private String indexName;
	private String fileName;
	private FetchType resultFetchType;
	private FetchType associatedFetchType;

	private Set<String> documentFields = Collections.emptySet();
	private Set<String> documentMaskedFields = Collections.emptySet();

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

	public Fetch setFileName(String fileName) {
		this.fileName = fileName;
		return this;
	}

	public String getFileFame() {
		return fileName;
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

	public Set<String> getDocumentFields() {
		return documentFields;
	}

	public void addDocumentField(String documentField) {
		if (documentFields.isEmpty()) {
			this.documentFields = new LinkedHashSet<>();
		}
		documentFields.add(documentField);
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
		if (fileName != null) {
			fetchRequestBuilder.setFileName(fileName);
		}
		if (resultFetchType != null) {
			fetchRequestBuilder.setResultFetchType(resultFetchType);
		}
		if (associatedFetchType != null) {
			fetchRequestBuilder.setAssociatedFetchType(associatedFetchType);
		}
		fetchRequestBuilder.addAllDocumentFields(documentFields);
		fetchRequestBuilder.addAllDocumentMaskedFields(documentMaskedFields);

		return fetchRequestBuilder.build();
	}

	@Override
	public FetchResult execute(LumongoConnection lumongoConnection) throws ServiceException {

		ExternalService.BlockingInterface service = lumongoConnection.getService();

		RpcController controller = lumongoConnection.getController();

		FetchResponse fetchResponse = service.fetch(controller, getRequest());

		return new FetchResult(fetchResponse);

	}

}
