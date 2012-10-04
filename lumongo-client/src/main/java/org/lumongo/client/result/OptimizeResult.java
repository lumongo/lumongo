package org.lumongo.client.result;

import org.lumongo.cluster.message.Lumongo.OptimizeResponse;

public class OptimizeResult extends Result {

    @SuppressWarnings("unused")
    private OptimizeResponse optimizeResponse;

    public OptimizeResult(OptimizeResponse optimizeResponse, long commandTimeMs) {
        super(commandTimeMs);
        this.optimizeResponse = optimizeResponse;
    }

}
