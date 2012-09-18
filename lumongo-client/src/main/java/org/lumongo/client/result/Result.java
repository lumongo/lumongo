package org.lumongo.client.result;

public class Result {

    private long commandTimeMs;

    public Result(long commandTimeMs) {
        this.commandTimeMs = commandTimeMs;
    }

    public long getCommandTimeMs() {
        return commandTimeMs;
    }
}
