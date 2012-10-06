package org.lumongo.client.pool;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.lumongo.client.command.CallableCommand;
import org.lumongo.client.command.Command;
import org.lumongo.client.result.Result;

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

	public <S, R extends Result> Future<R> executeAsync(Command<S, R> command) {
		CallableCommand<S, R> callableCommand = new CallableCommand<S, R>(lumongoPool, command);
		return executeAsync(callableCommand);
	}

	public <S, R extends Result> R execute(Command<S, R> command) throws Exception {
		CallableCommand<S, R> callableCommand = new CallableCommand<S, R>(lumongoPool, command);
		return execute(callableCommand);
	}

	@Override
	public void shutdown() throws Exception {
		super.shutdown();
		lumongoPool.close();
	}
}
