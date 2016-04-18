package org.lumongo.client.command;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.GetTermsResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.GetTermsRequest;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;

public class GetTerms extends SimpleCommand<GetTermsRequest, GetTermsResult> {
	
	private String indexName;
	private String fieldName;

	private Integer minDocFreq;
	private Integer minTermFreq;
	private String startTerm;
	private String endTerm;
	private String termFilter;
	private String termMatch;
	private Integer amount;

	public GetTerms(String indexName, String fieldName) {
		this.indexName = indexName;
		this.fieldName = fieldName;
	}

	public String getIndexName() {
		return indexName;
	}

	public GetTerms setIndexName(String indexName) {
		this.indexName = indexName;
		return this;
	}

	public String getFieldName() {
		return fieldName;
	}

	public GetTerms setFieldName(String fieldName) {
		this.fieldName = fieldName;
		return this;
	}

	public Integer getMinDocFreq() {
		return minDocFreq;
	}

	public GetTerms setMinDocFreq(Integer minDocFreq) {
		this.minDocFreq = minDocFreq;
		return this;
	}

	public Integer getMinTermFreq() {
		return minTermFreq;
	}

	public GetTerms setMinTermFreq(Integer minTermFreq) {
		this.minTermFreq = minTermFreq;
		return this;
	}

	public String getStartTerm() {
		return startTerm;
	}

	public GetTerms setStartTerm(String startTerm) {
		this.startTerm = startTerm;
		return this;
	}

	public String getEndTerm() {
		return endTerm;
	}

	public GetTerms setEndTerm(String endTerm) {
		this.endTerm = endTerm;
		return this;
	}

	public String getTermFilter() {
		return termFilter;
	}

	public GetTerms setTermFilter(String termFilter) {
		this.termFilter = termFilter;
		return this;
	}

	public String getTermMatch() {
		return termMatch;
	}

	public GetTerms setTermMatch(String termMatch) {
		this.termMatch = termMatch;
		return this;
	}

	public Integer getAmount() {
		return amount;
	}

	public GetTerms setAmount(Integer amount) {
		this.amount = amount;
		return this;
	}

	@Override
	public GetTermsRequest getRequest() {
		GetTermsRequest.Builder getTermsRequestBuilder = GetTermsRequest.newBuilder().setIndexName(indexName).setFieldName(fieldName);
		if (startTerm != null) {
			getTermsRequestBuilder.setStartTerm(startTerm);
		}
		if (endTerm != null) {
			getTermsRequestBuilder.setEndTerm(endTerm);
		}
		if (minDocFreq != null) {
			getTermsRequestBuilder.setMinDocFreq(minDocFreq);
		}
		if (minTermFreq != null) {
			getTermsRequestBuilder.setMinTermFreq(minTermFreq);
		}
		if (amount != null) {
			getTermsRequestBuilder.setAmount(amount);
		}
		if (termFilter != null) {
			getTermsRequestBuilder.setTermFilter(termFilter);
		}
		if (termMatch != null) {
			getTermsRequestBuilder.setTermMatch(termMatch);
		}
		return getTermsRequestBuilder.build();
	}
	
	@Override
	public GetTermsResult execute(LumongoConnection lumongoConnection) throws ServiceException {
		ExternalService.BlockingInterface service = lumongoConnection.getService();
		
		RpcController controller = lumongoConnection.getController();
		
		long start = System.currentTimeMillis();
		GetTermsResponse getTermsResponse = service.getTerms(controller, getRequest());
		long end = System.currentTimeMillis();
		long durationInMs = end - start;
		return new GetTermsResult(getTermsResponse, durationInMs);
	}
	
}
