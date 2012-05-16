package org.lumongo.client.config;

import java.util.ArrayList;
import java.util.List;

import org.lumongo.LumongoConstants;
import org.lumongo.client.LumongoClient;
import org.lumongo.cluster.message.Lumongo.LMMember;

/**
 * Contains all settings for the necessary for the {@link LumongoClient}
 * 
 * @author mdavis
 * 
 */
public class LumongoClientConfig {
	
	private List<LMMember> members;
	private int defaultRetries;
	
	public final static int DEFAULT_DEFAULT_RETRIES = 0;
	
	public LumongoClientConfig() {
		this.members = new ArrayList<LMMember>();
		this.defaultRetries = DEFAULT_DEFAULT_RETRIES;
	}
	
	public void addMember(String serverAddress) {
		addMember(serverAddress, LumongoConstants.DEFAULT_EXTERNAL_SERVICE_PORT);
	}
	
	public void addMember(String serverAddress, int externalPort) {
		LMMember member = LMMember.newBuilder().setServerAddress(serverAddress).setExternalPort(externalPort).build();
		members.add(member);
	}
	
	public void addMember(LMMember member) {
		members.add(member);
	}
	
	public void clearMembers() {
		members.clear();
	}
	
	public List<LMMember> getMembers() {
		return members;
	}
	
	public int getDefaultRetries() {
		return defaultRetries;
	}
	
	public void setDefaultRetries(int defaultRetries) {
		this.defaultRetries = defaultRetries;
	}
	
}
