package org.lumongo.client.command.base;

import java.util.concurrent.Callable;

import org.lumongo.client.pool.LumongoPool;
import org.lumongo.client.result.Result;

public class CallableCommand<R extends Result> implements Callable<R> {

	private Command<R> command;
	private LumongoPool lumongoPool;


	public CallableCommand(LumongoPool lumongoPool, Command<R> command) {
		this.lumongoPool = lumongoPool;
		this.command = command;
	}

	@Override
	public R call() throws Exception {
		return lumongoPool.execute(command);
	}
}

