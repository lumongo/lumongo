package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.IndexCreateResponse;

public class CreateIndexResult extends Result {

    @SuppressWarnings("unused")
    private IndexCreateResponse indexCreateResponse;

    public CreateIndexResult(IndexCreateResponse indexCreateResponse,long commandTimeMs) {
        super(commandTimeMs);

        this.indexCreateResponse = indexCreateResponse;
    }

}
