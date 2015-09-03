package org.lumongo.server.index;

import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import org.bson.BSON;
import org.lumongo.cluster.message.Lumongo;

/**
 * Created by mdavis on 8/15/15.
 */
public class ResultBundle {
	private BasicDBObject resultObj;
	private Lumongo.ResultDocument.Builder resultDocBuilder;

	public ResultBundle() {

	}

	public BasicDBObject getResultObj() {
		return resultObj;
	}

	public void setResultObj(BasicDBObject resultObj) {
		this.resultObj = resultObj;
	}

	public Lumongo.ResultDocument.Builder getResultDocBuilder() {
		return resultDocBuilder;
	}

	public void setResultDocBuilder(Lumongo.ResultDocument.Builder resultDocBuilder) {
		this.resultDocBuilder = resultDocBuilder;
	}

	public Lumongo.ResultDocument build() {
		updateBuilder();
		return resultDocBuilder.build();
	}

	public void updateBuilder() {
		if (resultObj != null) {
			ByteString document = ByteString.copyFrom(BSON.encode(resultObj));
			resultDocBuilder.setDocument(document);
		}
	}

	@Override
	public String toString() {
		return "ResultBundle{" +
				"resultObj=" + resultObj +
				", resultDocBuilder=" + resultDocBuilder +
				'}';
	}
}
