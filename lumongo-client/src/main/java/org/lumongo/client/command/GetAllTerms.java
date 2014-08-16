package org.lumongo.client.command;

import java.util.LinkedHashSet;
import java.util.Set;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.GetTermsResult;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.GetTermsResponse;

import com.google.protobuf.ServiceException;

public class GetAllTerms extends GetTerms {
	public static final int FETCH_SIZE = 64 * 1024;
	
	public GetAllTerms(String indexName, String fieldName) {
		super(indexName, fieldName, FETCH_SIZE);
	}
	
	@Override
	public GetTermsResult execute(LumongoConnection lumongoConnection) throws ServiceException {
		GetTermsResponse.Builder fullResponse = GetTermsResponse.newBuilder();
		Set<Lumongo.Term> terms = new LinkedHashSet<Lumongo.Term>();
		
		long start = System.currentTimeMillis();
		
		Lumongo.Term currentStartTerm = null;
		Lumongo.Term nextStartTerm = null;
		
		if (getStartTerm() != null) {
			nextStartTerm = Lumongo.Term.newBuilder().setValue(getStartTerm()).build();
		}
		
		do {
			currentStartTerm = nextStartTerm;
			if (currentStartTerm != null) {
				setStartTerm(currentStartTerm.getValue());
			}
			GetTermsResult gtr = super.execute(lumongoConnection);
			terms.addAll(gtr.getTerms());
			
			nextStartTerm = gtr.getLastTerm();
			
		}
		while (nextStartTerm != null && !nextStartTerm.equals(currentStartTerm));
		
		long end = System.currentTimeMillis();
		long durationInMs = end - start;
		
		fullResponse.addAllTerm(terms);
		
		return new GetTermsResult(fullResponse.build(), durationInMs);
	}
	
}
