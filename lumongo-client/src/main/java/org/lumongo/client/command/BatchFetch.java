package org.lumongo.client.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.lumongo.client.command.base.SimpleCommand;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.BatchFetchResult;
import org.lumongo.cluster.message.Lumongo.ExternalService;
import org.lumongo.cluster.message.Lumongo.FetchRequest.FetchType;
import org.lumongo.cluster.message.Lumongo.GroupFetchRequest;
import org.lumongo.cluster.message.Lumongo.GroupFetchResponse;
import org.lumongo.cluster.message.Lumongo.ScoredResult;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Fetches multiple documents in a single call
 * @author mdavis
 *
 */
public class BatchFetch extends SimpleCommand<GroupFetchRequest, BatchFetchResult> {

    private List<Fetch> fetchList;

    public BatchFetch() {
        this.fetchList = new ArrayList<Fetch>();
    }

    public BatchFetch addFetches(Collection<? extends Fetch> fetches) {
        this.fetchList.addAll(fetches);
        return this;
    }

    public BatchFetch addFetchDocumentsFromUniqueIds(Collection<String> uniqueIds) {

        for (String uniqueId : uniqueIds) {
            Fetch f = new Fetch(uniqueId);
            f.setResultFetchType(FetchType.FULL);
            f.setAssociatedFetchType(FetchType.NONE);
            fetchList.add(f);
        }
        return this;
    }

    public BatchFetch addFetchDocumentsFromResults(Collection<ScoredResult> scoredResults) {

        for (ScoredResult scoredResult : scoredResults) {
            Fetch f = new Fetch(scoredResult.getUniqueId());
            f.setResultFetchType(FetchType.FULL);
            f.setAssociatedFetchType(FetchType.NONE);
            fetchList.add(f);
        }
        return this;
    }


    @Override
    public GroupFetchRequest getRequest() {
        GroupFetchRequest.Builder groupFetchRequestBuilder = GroupFetchRequest.newBuilder();
        for (Fetch f : fetchList) {
            groupFetchRequestBuilder.addFetchRequest(f.getRequest());
        }
        return groupFetchRequestBuilder.build();
    }

    @Override
    public BatchFetchResult execute(LumongoConnection lumongoConnection) throws ServiceException {
        ExternalService.BlockingInterface service = lumongoConnection.getService();
        RpcController controller = lumongoConnection.getController();

        GroupFetchResponse groupFetchResponse = service.groupFetch(controller, getRequest());

        return new BatchFetchResult(groupFetchResponse);
    }

}
