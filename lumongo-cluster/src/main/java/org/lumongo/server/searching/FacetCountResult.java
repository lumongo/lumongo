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
        int compareCount = Long.compare(this.count, o.count) * -1;
        if (compareCount == 0) {
            return this.facet.compareTo(o.facet);
        }
        return compareCount;
    }

	    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FacetCountResult) {
            return (compareTo((FacetCountResult) obj) == 0);
        }
        return false;
    }
    
    @Override
    public int hashCode() {     
        return facet.hashCode() + (int)count;
    }
}
