package org.lumongo.util;

public class SegmentUtil {
	public static int findSegmentForUniqueId(String uniqueId, int numSegments) {
		int segmentNumber = Math.abs(uniqueId.hashCode()) % numSegments;
		return segmentNumber;
	}
}
