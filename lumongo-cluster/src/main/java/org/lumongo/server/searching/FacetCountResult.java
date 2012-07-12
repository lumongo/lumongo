package org.lumongo.server.searching;

public class FacetCountResult implements Comparable<FacetCountResult> {
	private String facet;
	private long count;
	
	public FacetCountResult(String facet, long count) {
		this.facet = facet;
		this.count = count;
	}
	
	public String getFacet() {
		return facet;
	}
	
	public void setFacet(String facet) {
		this.facet = facet;
	}
	
	public long getCount() {
		return count;
	}
	
	public void setCount(long count) {
		this.count = count;
	}
	
	@Override
	public int compareTo(FacetCountResult o) {
		return Long.compare(this.count, o.count);
	}
	
}
