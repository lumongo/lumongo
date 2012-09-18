package org.lumongo.client.pool;

import java.util.concurrent.Future;

import org.lumongo.client.command.Command;
import org.lumongo.client.command.CallableCommand;

public class LumongoWorkPool extends WorkPool {

    private LumongoPool lumongoPool;

    public LumongoWorkPool(LumongoPool lumongoPool, int threads, int maxQueued, String poolName) {
        super(threads, maxQueued, poolName);
        this.lumongoPool = lumongoPool;
    }

    public <R> Future<R> executeAsync(Command<R> command) {
        CallableCommand<R> callableCommand = new CallableCommand<R>(lumongoPool, command);
        return executeAsync(callableCommand);
    }
}
