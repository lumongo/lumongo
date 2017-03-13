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
	private String lumongoVersion;
	private String luceneVersion;
	private Long serverMemory;
	private Long lumongoMemory;
	private Long diskSize;

	public List<IndexInfo> getIndexes() {
		return indexes;
	}

	public void setIndexes(List<IndexInfo> indexes) {
		this.indexes = indexes;
	}

	public String getLumongoVersion() {
		return lumongoVersion;
	}

	public void setLumongoVersion(String lumongoVersion) {
		this.lumongoVersion = lumongoVersion;
	}

	public String getLuceneVersion() {
		return luceneVersion;
	}

	public void setLuceneVersion(String luceneVersion) {
		this.luceneVersion = luceneVersion;
	}

	public Long getServerMemory() {
		return serverMemory;
	}

	public void setServerMemory(Long serverMemory) {
		this.serverMemory = serverMemory;
	}

	public Long getLumongoMemory() {
		return lumongoMemory;
	}

	public void setLumongoMemory(Long lumongoMemory) {
		this.lumongoMemory = lumongoMemory;
	}

	public Long getDiskSize() {
		return diskSize;
	}

	public void setDiskSize(Long diskSize) {
		this.diskSize = diskSize;
	}

	@Override
	public String toString() {
		return "InstanceInfo{" + "indexes=" + indexes + ", lumongoVersion='" + lumongoVersion + '\'' + ", luceneVersion='" + luceneVersion + '\''
				+ ", serverMemory=" + serverMemory + ", lumongoMemory=" + lumongoMemory + ", diskSize=" + diskSize + '}';
	}
}
