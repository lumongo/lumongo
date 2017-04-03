package org.lumongo.ui.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by Payam Meyer on 3/10/17.
 * @author pmeyer
 */
public class IndexInfo implements IsSerializable {

	private String name;
	private List<String> qfList = Collections.emptyList();
	private List<String> flList = Collections.emptyList();
	private List<String> facetList = Collections.emptyList();
	private Long size;
	private Integer totalDocs;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getQfList() {
		if (qfList.isEmpty()) {
			qfList = new ArrayList<>();
		}
		return qfList;
	}

	public void setQfList(List<String> qfList) {
		this.qfList = qfList;
	}

	public List<String> getFlList() {
		if (flList.isEmpty()) {
			flList = new ArrayList<>();
		}
		return flList;
	}

	public void setFlList(List<String> flList) {
		this.flList = flList;
	}

	public List<String> getFacetList() {
		if (facetList.isEmpty()) {
			facetList = new ArrayList<>();
		}
		return facetList;
	}

	public void setFacetList(List<String> facetList) {

		this.facetList = facetList;
	}

	public Long getSize() {
		return size;
	}

	public void setSize(Long size) {
		this.size = size;
	}

	public Integer getTotalDocs() {
		return totalDocs;
	}

	public void setTotalDocs(Integer totalDocs) {
		this.totalDocs = totalDocs;
	}

	@Override
	public String toString() {
		return "IndexInfo{" + "name='" + name + '\'' + ", qfList=" + qfList + ", size=" + size + ", totalDocs=" + totalDocs + '}';
	}
}
