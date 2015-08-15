package org.lumongo.util.cache;

public class DocId {
	private String uniqueId;
	private String indexName;

	public DocId(String uniqueId, String indexName) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
	}

	public String getUniqueId() {
		return uniqueId;
	}

	public String getIndexName() {
		return indexName;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((indexName == null) ? 0 : indexName.hashCode());
		result = prime * result + ((uniqueId == null) ? 0 : uniqueId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof DocId)) {
			return false;
		}
		DocId other = (DocId) obj;
		if (indexName == null) {
			if (other.indexName != null) {
				return false;
			}
		}
		else if (!indexName.equals(other.indexName)) {
			return false;
		}
		if (uniqueId == null) {
			if (other.uniqueId != null) {
				return false;
			}
		}
		else if (!uniqueId.equals(other.uniqueId)) {
			return false;
		}
		return true;
	}
}