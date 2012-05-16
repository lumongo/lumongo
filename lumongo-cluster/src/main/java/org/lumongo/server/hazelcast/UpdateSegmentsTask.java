package org.lumongo.server.hazelcast;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.hazelcast.core.Member;

public class UpdateSegmentsTask implements Callable<Void>, Serializable {
	
	private static Logger log = Logger.getLogger(UpdateSegmentsTask.class);
	
	private static final long serialVersionUID = 1L;
	
	private final int hazelcastPort;
	
	private final String indexName;
	private final Map<Member, Set<Integer>> newMemberToSegmentMap;
	
	public UpdateSegmentsTask(int hazelcastPort, String indexName, Map<Member, Set<Integer>> newMemberToSegmentMap) {
		this.hazelcastPort = hazelcastPort;
		this.indexName = indexName;
		this.newMemberToSegmentMap = newMemberToSegmentMap;
	}
	
	@Override
	public Void call() throws Exception {
		try {
			HazelcastManager hazelcastManger = HazelcastManager.getHazelcastManager(hazelcastPort);
			hazelcastManger.updateSegmentMap(indexName, newMemberToSegmentMap);
		}
		catch (Exception e) {
			log.error(e.getClass().getSimpleName() + ": ", e);
			throw e;
		}
		return null;
	}
}
