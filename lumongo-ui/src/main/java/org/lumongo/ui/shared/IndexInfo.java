package org.lumongo.ui.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.List;

/**
 * Created by Payam Meyer on 3/10/17.
 * @author pmeyer
 */
public class IndexInfo implements IsSerializable {

	private String name;
	private List<String> fieldNames;
	private Double size;
	private Integer totalDocs;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<String> getFieldNames() {
		return fieldNames;
	}

	public void setFieldNames(List<String> fieldNames) {
		this.fieldNames = fieldNames;
	}

	public Double getSize() {
		return size;
	}

	public void setSize(Double size) {
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
		return "IndexInfo{" + "name='" + name + '\'' + ", fieldNames=" + fieldNames + ", size=" + size + ", totalDocs=" + totalDocs + '}';
	}
}
