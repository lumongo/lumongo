package org.lumongo.server.connection;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.BasicBSONObject;
import org.lumongo.cluster.message.ExternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.*;
import org.lumongo.server.config.IndexConfig;
import org.lumongo.server.index.LumongoIndexManager;
import org.lumongo.util.cache.MetaKeys;

import java.net.UnknownHostException;

public class ExternalServiceHandler extends ExternalServiceGrpc.ExternalServiceImplBase {
	private final static Logger log = Logger.getLogger(ExternalServiceHandler.class);

	private final LumongoIndexManager indexManger;

	public ExternalServiceHandler(LumongoIndexManager indexManger) throws UnknownHostException {
		this.indexManger = indexManger;
	}

	@Override
	public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
		try {
			responseObserver.onNext(indexManger.query(request));
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to run query: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			Metadata m = new Metadata();
			m.put(MetaKeys.ERROR_KEY, e.getMessage());
			responseObserver.onError(new StatusRuntimeException(Status.UNKNOWN, m));
		}

	}

	@Override
	public void store(StoreRequest request, StreamObserver<StoreResponse> responseObserver) {
		try {
			responseObserver.onNext(indexManger.storeDocument(request));
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to store: <" + request.getUniqueId() + "> in index <" + request.getIndexName() + ">: " + e.getClass().getSimpleName() + ": ", e);
			if (request.hasResultDocument()) {
				try {
					if (request.getResultDocument().hasDocument()) {
						BasicBSONObject document = (BasicBSONObject) BSON.decode(request.getResultDocument().getDocument().toByteArray());
						log.error(document.toString());
					}
				}
				catch (Exception e2) {

				}
			}

			responseObserver.onError(e);
		}
	}

	@Override
	public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
		try {
			responseObserver.onNext(indexManger.deleteDocument(request));
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to delete: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void fetch(FetchRequest request, StreamObserver<FetchResponse> responseObserver) {
		try {
			responseObserver.onNext(indexManger.fetch(request));
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to fetch: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void createIndex(IndexCreateRequest request, StreamObserver<IndexCreateResponse> responseObserver) {
		try {
			responseObserver.onNext(indexManger.createIndex(request));
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to create index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}

	}

	@Override
	public void changeIndex(IndexSettingsRequest request, StreamObserver<IndexSettingsResponse> responseObserver) {
		try {
			IndexSettingsResponse r = indexManger.updateIndex(request.getIndexName(), request.getIndexSettings());
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to change index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}

	}

	@Override
	public void deleteIndex(IndexDeleteRequest request, StreamObserver<IndexDeleteResponse> responseObserver) {
		try {
			IndexDeleteResponse r = indexManger.deleteIndex(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to delete index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getIndexes(GetIndexesRequest request, StreamObserver<GetIndexesResponse> responseObserver) {
		try {
			GetIndexesResponse r = indexManger.getIndexes(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to get indexes: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getNumberOfDocs(GetNumberOfDocsRequest request, StreamObserver<GetNumberOfDocsResponse> responseObserver) {
		try {
			GetNumberOfDocsResponse r = indexManger.getNumberOfDocs(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to get number of docs: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
		try {
			ClearResponse r = indexManger.clearIndex(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to clear index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void optimize(OptimizeRequest request, StreamObserver<OptimizeResponse> responseObserver) {
		try {
			OptimizeResponse r = indexManger.optimize(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to optimize index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getFieldNames(GetFieldNamesRequest request, StreamObserver<GetFieldNamesResponse> responseObserver) {
		try {
			GetFieldNamesResponse r = indexManger.getFieldNames(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to get field names: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getTerms(GetTermsRequest request, StreamObserver<GetTermsResponse> responseObserver) {
		try {
			GetTermsResponse r = indexManger.getTerms(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to get terms: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getMembers(GetMembersRequest request, StreamObserver<GetMembersResponse> responseObserver) {
		try {
			GetMembersResponse r = indexManger.getMembers(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to get members: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getIndexConfig(Lumongo.GetIndexConfigRequest request, StreamObserver<Lumongo.GetIndexConfigResponse> responseObserver) {
		try {
			IndexConfig indexConfig = indexManger.getIndexConfig(request.getIndexName());
			GetIndexConfigResponse r = GetIndexConfigResponse.newBuilder().setIndexSettings(indexConfig.getIndexSettings()).build();
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to get get index config: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void batchFetch(BatchFetchRequest request, StreamObserver<BatchFetchResponse> responseObserver) {
		try {
			BatchFetchResponse.Builder gfrb = BatchFetchResponse.newBuilder();
			for (FetchRequest fr : request.getFetchRequestList()) {
				FetchResponse res = indexManger.fetch(fr);
				gfrb.addFetchResponse(res);
			}
			BatchFetchResponse r = gfrb.build();
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to group fetch: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void batchDelete(BatchDeleteRequest request, StreamObserver<BatchDeleteResponse> responseObserver) {
		try {

			for (DeleteRequest dr : request.getRequestList()) {
				@SuppressWarnings("unused") DeleteResponse res = indexManger.deleteDocument(dr);
			}
			BatchDeleteResponse r = BatchDeleteResponse.newBuilder().build();
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to batch delete: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

}
