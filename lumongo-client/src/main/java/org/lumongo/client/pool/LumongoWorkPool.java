package org.lumongo.client.pool;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.lumongo.client.command.CallableCommand;
import org.lumongo.client.command.Command;
import org.lumongo.client.result.Result;
import org.lumongo.cluster.message.Lumongo.LMMember;

public class LumongoWorkPool extends WorkPool {

    private LumongoPool lumongoPool;

    private static AtomicInteger counter = new AtomicInteger(0);

    public LumongoWorkPool(LumongoPool lumongoPool) {
        this(lumongoPool, "lumongoPool-" + counter.getAndIncrement());
    }

    public LumongoWorkPool(LumongoPool lumongoPool, String poolName) {
        super(lumongoPool.getMaxConnections(), lumongoPool.getMaxConnections() * 2, poolName);
        this.lumongoPool = lumongoPool;
    }

    public <R extends Result> Future<R> executeAsync(Command<R> command) {
        CallableCommand<R> callableCommand = new CallableCommand<R>(lumongoPool, command);
        return executeAsync(callableCommand);
    }

    public <R extends Result> R execute(Command<R> command) throws Exception {
        CallableCommand<R> callableCommand = new CallableCommand<R>(lumongoPool, command);
        return execute(callableCommand);
    }

    public void updateMembers() throws Exception {
        lumongoPool.updateMembers();
    }

    public void updateMembers(List<LMMember> members) throws Exception {
        lumongoPool.updateMembers(members);
    }

    @Override
    public void shutdown() throws Exception {
        super.shutdown();
        lumongoPool.close();
    }
}
