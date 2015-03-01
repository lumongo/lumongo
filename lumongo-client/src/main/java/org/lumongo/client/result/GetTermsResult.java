package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.GetTermsResponse;
import org.lumongo.cluster.message.Lumongo.Term;

import java.util.List;

public class GetTermsResult extends Result {

	private GetTermsResponse getTermsResponse;

	public GetTermsResult(GetTermsResponse getTermsResponse, long commandTimeMs) {
		this.getTermsResponse = getTermsResponse;
	}

	public List<Term> getTerms() {
		return getTermsResponse.getTermList();
	}

	public Term getLastTerm() {
		if (getTermsResponse.hasLastTerm()) {
			return getTermsResponse.getLastTerm();
		}
		return null;
	}

	@Override
	public String toString() {
		return getTermsResponse.toString();
	}

}
