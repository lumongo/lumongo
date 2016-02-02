package org.lumongo.client.command;

import org.lumongo.cluster.message.Lumongo;

import java.nio.charset.Charset;
import java.util.Base64;

/**
 * Created by Matt Davis on 2/2/16.
 * @author mdavis
 */
public class CursorHelper {

	private static Charset utf8Charset = Charset.forName("utf-8");

	public static String getStaticIndexCursor(Lumongo.LastResult lastResult) {
		return new String(Base64.getEncoder().encode(lastResult.toByteArray()), utf8Charset);
	}

	public static String getUniqueSortedCursor(Lumongo.LastResult lastResult) {

		Lumongo.LastResult.Builder lastResultBuilder = Lumongo.LastResult.newBuilder();
		for (Lumongo.LastIndexResult lastIndexResult : lastResult.getLastIndexResultList()) {
			Lumongo.LastIndexResult.Builder lastIndexResultBuilder = Lumongo.LastIndexResult.newBuilder();
			lastIndexResultBuilder.setIndexName(lastIndexResult.getIndexName());
			for (Lumongo.ScoredResult scoredResult : lastIndexResult.getLastForSegmentList()) {
				Lumongo.ScoredResult.Builder scoredResultBuilder = Lumongo.ScoredResult.newBuilder(scoredResult).clearScore().clearResultDocument();
				lastIndexResultBuilder.addLastForSegment(scoredResultBuilder);
			}
			lastResultBuilder.addLastIndexResult(lastIndexResultBuilder);
		}

		return new String(Base64.getEncoder().encode(lastResultBuilder.build().toByteArray()), utf8Charset);
	}

	public static Lumongo.LastResult getLastResultFromCursor(String cursor) {
		try {
			return Lumongo.LastResult.parseFrom(Base64.getDecoder().decode(cursor.getBytes(utf8Charset)));
		}
		catch (Exception e) {
			throw new RuntimeException("Invalid cursor");
		}
	}
}
