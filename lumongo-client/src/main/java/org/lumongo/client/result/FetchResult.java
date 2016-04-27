package org.lumongo.client.result;

import com.google.protobuf.ByteString;
import org.bson.BsonBinaryReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.lumongo.cluster.message.Lumongo.AssociatedDocument;
import org.lumongo.cluster.message.Lumongo.FetchResponse;
import org.lumongo.cluster.message.Lumongo.Metadata;
import org.lumongo.cluster.message.Lumongo.ResultDocument;
import org.lumongo.fields.Mapper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FetchResult extends Result {
	
	private FetchResponse fetchResponse;
	
	private List<AssociatedResult> associatedResults;
	
	public FetchResult(FetchResponse fetchResponse) {
		this.fetchResponse = fetchResponse;
		
		if (fetchResponse.getAssociatedDocumentCount() == 0) {
			this.associatedResults = Collections.emptyList();
		}
		else {
			this.associatedResults = new ArrayList<>();
			for (AssociatedDocument ad : fetchResponse.getAssociatedDocumentList()) {
				associatedResults.add(new AssociatedResult(ad));
			}
		}
	}
	
	public ResultDocument getResultDocument() {
		return fetchResponse.getResultDocument();
	}
	
	public String getUniqueId() {
		if (fetchResponse.hasResultDocument()) {
			return fetchResponse.getResultDocument().getUniqueId();
		}
		return null;
	}
	
	public String getIndexName() {
		if (fetchResponse.hasResultDocument()) {
			return fetchResponse.getResultDocument().getIndexName();
		}
		return null;
	}
	
	public Map<String, String> getMeta() {
		if (fetchResponse.hasResultDocument()) {
			ResultDocument rd = fetchResponse.getResultDocument();
			HashMap<String, String> metadata = new HashMap<String, String>();
			for (Metadata md : rd.getMetadataList()) {
				metadata.put(md.getKey(), md.getValue());
			}
			return metadata;
		}
		return null;
	}

	public Document getDocument() {
		if (fetchResponse.hasResultDocument()) {
			ResultDocument rd = fetchResponse.getResultDocument();

			ByteString bs = rd.getDocument();
			BsonBinaryReader bsonReader = new BsonBinaryReader(ByteBuffer.wrap(bs.toByteArray()));
			return new DocumentCodec().decode(bsonReader, DecoderContext.builder().build());

		}
		return null;
	}
	
	public <T> T getDocument(Mapper<T> mapper) throws Exception {
		if (fetchResponse.hasResultDocument()) {
			Document document = getDocument();
			return mapper.fromDocument(document);
		}
		return null;
	}
	
	public AssociatedResult getAssociatedDocument(int index) {
		return associatedResults.get(index);
	}
	
	public List<AssociatedResult> getAssociatedDocuments() {
		return associatedResults;
	}
	
	public int getAssociatedDocumentCount() {
		return associatedResults.size();
	}
	
	public boolean hasResultDocument() {
		return fetchResponse.hasResultDocument();
	}
	
	public Long getDocumentTimestamp() {
		if (fetchResponse.hasResultDocument()) {
			ResultDocument rd = fetchResponse.getResultDocument();
			return rd.getTimestamp();
		}
		return null;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("\nFetchResult: {\n");
		if (hasResultDocument()) {
			sb.append("  ResultDocument: {\n");
			sb.append("    uniqueId: ");
			sb.append(getUniqueId());
			sb.append("\n");
			
			sb.append("    document: ");
			sb.append(getDocument());
			sb.append("\n  }\n");
			
		}
		
		for (AssociatedResult ad : getAssociatedDocuments()) {
			sb.append("  AssociatedDocument: {\n");
			sb.append("    filename: ");
			sb.append(ad.getFilename());
			sb.append("\n  }\n");
		}
		
		sb.append("}\n");
		return sb.toString();
	}
	
}
