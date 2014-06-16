package org.lumongo.server.indexing;

import org.lumongo.cluster.message.Lumongo.QueryRequest;

public class QueryCacheKey {
	
	private QueryRequest queryRequest;
	
	public QueryCacheKey(QueryRequest queryRequest) {
		this.queryRequest = queryRequest;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((queryRequest == null) ? 0 : queryRequest.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueryCacheKey other = (QueryCacheKey) obj;
		if (queryRequest == null) {
			if (other.queryRequest != null)
				return false;
		}
		else if (!queryRequest.equals(other.queryRequest))
			return false;
		return true;
	}
	
}
