package org.lumongo.client.pool;

import org.lumongo.cluster.message.Lumongo.IndexMapping;
import org.lumongo.cluster.message.Lumongo.LMMember;
import org.lumongo.cluster.message.Lumongo.SegmentMapping;
import org.lumongo.util.SegmentUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexRouting {
	private Map<String, Map<Integer, LMMember>> indexMapping = new HashMap<String, Map<Integer, LMMember>>();
	private Map<String, Integer> segmentCountMapping = new HashMap<String, Integer>();

	public IndexRouting(List<IndexMapping> indexMappingList) {
		for (IndexMapping im : indexMappingList) {
			Map<Integer, LMMember> segmentMapping = new HashMap<Integer, LMMember>();
			for (SegmentMapping sg : im.getSegmentMappingList()) {
				segmentMapping.put(sg.getSegmentNumber(), sg.getMember());
			}
			segmentCountMapping.put(im.getIndexName(), im.getNumberOfSegments());
			indexMapping.put(im.getIndexName(), segmentMapping);
		}
	}

	public LMMember getMember(String indexName, String uniqueId) {
		Integer numberOfSegments = segmentCountMapping.get(indexName);
		if (numberOfSegments == null) {
			return null;
		}

		Map<Integer, LMMember> segmentMapping = indexMapping.get(indexName);

		int segmentNumber = SegmentUtil.findSegmentForUniqueId(uniqueId, numberOfSegments);
		return segmentMapping.get(segmentNumber);
	}
}
