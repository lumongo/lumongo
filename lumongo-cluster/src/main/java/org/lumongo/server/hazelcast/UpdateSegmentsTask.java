package org.lumongo.server.hazelcast;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class UpdateSegmentsTask implements Callable<Void>, DataSerializable {
	
	private static final long serialVersionUID = 1L;
	private static Logger log = Logger.getLogger(UpdateSegmentsTask.class);
	private int hazelcastPort;
	
	private String indexName;
	private Map<Member, Set<Integer>> newMemberToSegmentMap;

	public UpdateSegmentsTask() {

	}

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

	@Override
	public void writeData(ObjectDataOutput out) throws IOException {
		out.writeInt(hazelcastPort);
		out.writeUTF(indexName);
		out.writeInt(newMemberToSegmentMap.size());

		for (Map.Entry<Member, Set<Integer>> e : newMemberToSegmentMap.entrySet()) {
			e.getKey().writeData(out);
			out.writeInt(e.getValue().size());
			for (Integer segment : e.getValue()) {
				out.writeInt(segment);
			}
		}

	}

	@Override
	public void readData(ObjectDataInput in) throws IOException {
		hazelcastPort = in.readInt();
		indexName = in.readUTF();
		newMemberToSegmentMap = new HashMap<>();
		int count = in.readInt();

		for (int i = 0; i < count; i++) {
			MemberImpl mi = new MemberImpl();
			mi.readData(in);
			Set<Integer> segments = new HashSet<>();
			int segCount = in.readInt();
			for (int j = 0; j < segCount; j++) {
				segments.add(in.readInt());
			}
			newMemberToSegmentMap.put(mi, segments);
		}

	}
}
