package org.lumongo.server.searching;

import java.util.Comparator;

public class FacetCountResult implements Comparable<FacetCountResult> {
	private String facet;
	private long count;

	public static Comparator<FacetCountResult> COUNT_THEN_FACET_COMPARE = Comparator.comparingLong(FacetCountResult::getCount).reversed().thenComparing(FacetCountResult::getFacet);

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
		return COUNT_THEN_FACET_COMPARE.compare(this, o);
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
		return facet.hashCode() + Long.hashCode(count);
	}
}
