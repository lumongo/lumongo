package org.lumongo.client.command;

import java.util.concurrent.Callable;

import org.lumongo.client.pool.LumongoPool;

public class CallableCommand<S, R> implements Callable<R> {

    private Command<S, R> command;
    private LumongoPool lumongoPool;


    public CallableCommand(LumongoPool lumongoPool, Command<S, R> command) {
        this.lumongoPool = lumongoPool;
        this.command = command;
    }

    @Override
    public R call() throws Exception {
        return lumongoPool.execute(command);
    }
}

