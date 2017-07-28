package org.lumongo.server.connection;

import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;
import org.lumongo.cluster.message.InternalServiceGrpc;
import org.lumongo.cluster.message.Lumongo;
import org.lumongo.cluster.message.Lumongo.*;
import org.lumongo.server.index.LumongoIndexManager;

public class InternalServiceHandler extends InternalServiceGrpc.InternalServiceImplBase {

	private final static Logger log = Logger.getLogger(InternalServiceHandler.class);

	private final LumongoIndexManager indexManager;

	public InternalServiceHandler(LumongoIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@Override
	public void query(QueryRequest request, StreamObserver<InternalQueryResponse> responseObserver) {
		try {
			InternalQueryResponse r = indexManager.internalQuery(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to run internal query: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void store(StoreRequest request, StreamObserver<StoreResponse> responseObserver) {
		try {
			StoreResponse r = indexManager.storeInternal(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to run internal store: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void fetch(Lumongo.FetchRequest request, StreamObserver<Lumongo.FetchResponse> responseObserver) {
		try {
			Lumongo.FetchResponse r = indexManager.internalFetch(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to run internal fetch: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
		try {
			DeleteResponse r = indexManager.internalDeleteDocument(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to run internal delete: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getNumberOfDocs(GetNumberOfDocsRequest request, StreamObserver<GetNumberOfDocsResponse> responseObserver) {
		try {
			GetNumberOfDocsResponse r = indexManager.getNumberOfDocsInternal(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to run get number of docs: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
		try {
			ClearResponse r = indexManager.clearInternal(request);
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
			OptimizeResponse r = indexManager.optimizeInternal(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to optimized index: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getFieldNames(GetFieldNamesRequest request, StreamObserver<GetFieldNamesResponse> responseObserver) {
		try {
			GetFieldNamesResponse r = indexManager.getFieldNamesInternal(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to get field names: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

	@Override
	public void getTerms(GetTermsRequest request, StreamObserver<Lumongo.GetTermsResponseInternal> responseObserver) {
		try {
			Lumongo.GetTermsResponseInternal r = indexManager.getTermsInternal(request);
			responseObserver.onNext(r);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			log.error("Failed to get terms: <" + request + ">: " + e.getClass().getSimpleName() + ": ", e);
			responseObserver.onError(e);
		}
	}

}
