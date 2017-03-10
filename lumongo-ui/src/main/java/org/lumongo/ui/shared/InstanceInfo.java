package org.lumongo.ui.shared;

import com.google.gwt.user.client.rpc.IsSerializable;

import java.util.Collections;
import java.util.List;

/**
 * Created by Payam Meyer on 3/10/17.
 * @author pmeyer
 */
public class InstanceInfo implements IsSerializable {

	private List<IndexInfo> indexes = Collections.emptyList();
	private Double serverMemory;
	private Double lumongoMemory;
	private Double diskSize;

	public List<IndexInfo> getIndexes() {
		return indexes;
	}

	public void setIndexes(List<IndexInfo> indexes) {
		this.indexes = indexes;
	}

	public Double getServerMemory() {
		return serverMemory;
	}

	public void setServerMemory(Double serverMemory) {
		this.serverMemory = serverMemory;
	}

	public Double getLumongoMemory() {
		return lumongoMemory;
	}

	public void setLumongoMemory(Double lumongoMemory) {
		this.lumongoMemory = lumongoMemory;
	}

	public Double getDiskSize() {
		return diskSize;
	}

	public void setDiskSize(Double diskSize) {
		this.diskSize = diskSize;
	}

	@Override
	public String toString() {
		return "InstanceInfo{" + "indexes=" + indexes + ", serverMemory=" + serverMemory + ", lumongoMemory=" + lumongoMemory + ", diskSize=" + diskSize + '}';
	}
}
