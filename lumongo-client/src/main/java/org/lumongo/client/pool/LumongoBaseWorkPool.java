package org.lumongo.client.pool;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.lumongo.client.command.base.CallableCommand;
import org.lumongo.client.command.base.Command;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.result.Result;
import org.lumongo.cluster.message.Lumongo.LMMember;

import com.google.common.util.concurrent.ListenableFuture;

public class LumongoBaseWorkPool extends WorkPool {

    private LumongoPool lumongoPool;

    private static AtomicInteger counter = new AtomicInteger(0);

    public LumongoBaseWorkPool(LumongoPoolConfig lumongoPoolConfig) {
        this(new LumongoPool(lumongoPoolConfig), lumongoPoolConfig.getPoolName() != null ? lumongoPoolConfig.getPoolName() : "lumongoPool-"
                + counter.getAndIncrement());
    }

    public LumongoBaseWorkPool(LumongoPool lumongoPool) {
        this(lumongoPool, "lumongoPool-" + counter.getAndIncrement());
    }

    public LumongoBaseWorkPool(LumongoPool lumongoPool, String poolName) {
        super(lumongoPool.getMaxConnections(), lumongoPool.getMaxConnections() * 10, poolName);
        this.lumongoPool = lumongoPool;
    }

    public <R extends Result> ListenableFuture<R> executeAsync(Command<R> command) {
        CallableCommand<R> callableCommand = new CallableCommand<R>(lumongoPool, command);
        return executeAsync(callableCommand);
    }

    public <R extends Result> R execute(Command<R> command) throws Exception {
        CallableCommand<R> callableCommand = new CallableCommand<R>(lumongoPool, command);
        return execute(callableCommand);
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
