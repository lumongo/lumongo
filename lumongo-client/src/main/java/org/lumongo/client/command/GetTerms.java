package org.lumongo.client.command;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.GetTermsResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.GetTermsRequest;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

public class GetTerms extends SimpleCommand<GetTermsRequest, GetTermsResult> {
	
	private String indexName;
	private String fieldName;
	private int amount;
	
	private Integer minDocFreq;
	private String startTerm;
	private Boolean realTime;
	private String termFilter;
	private String termMatch;
	
	public GetTerms(String indexName, String fieldName, int amount) {
		this.indexName = indexName;
		this.fieldName = fieldName;
		this.amount = amount;
	}
	
	public GetTerms setMinDocFreq(Integer minDocFreq) {
		this.minDocFreq = minDocFreq;
		return this;
	}
	
	public Integer getMinDocFreq() {
		return minDocFreq;
	}
	
	public GetTerms setStartTerm(String startTerm) {
		this.startTerm = startTerm;
		return this;
	}
	
	public String getStartTerm() {
		return startTerm;
	}
	
	public GetTerms setRealTime(Boolean realTime) {
		this.realTime = realTime;
		return this;
	}
	
	public Boolean getRealTime() {
		return realTime;
	}
	
	public GetTerms setTermFilter(String termFilter) {
		this.termFilter = termFilter;
		return this;
	}
	
	public String getTermFilter() {
		return termFilter;
	}
	
	public GetTerms setTermMatch(String termMatch) {
		this.termMatch = termMatch;
		return this;
	}
	
	public String getTermMatch() {
		return termMatch;
	}
	
	@Override
	public GetTermsRequest getRequest() {
		GetTermsRequest.Builder getTermsRequestBuilder = GetTermsRequest.newBuilder().setIndexName(indexName).setFieldName(fieldName).setAmount(amount);
		if (startTerm != null) {
			getTermsRequestBuilder.setStartingTerm(startTerm);
		}
		if (realTime != null) {
			getTermsRequestBuilder.setRealTime(realTime);
		}
		if (minDocFreq != null) {
			getTermsRequestBuilder.setMinDocFreq(minDocFreq);
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
