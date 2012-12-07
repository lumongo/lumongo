package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.Result;

public abstract class Command<R extends Result> {


    public abstract R execute(LumongoConnection lumongoConnection) throws Exception;

    public R executeTimed(LumongoConnection lumongoConnection) throws Exception {
        long start = System.currentTimeMillis();
        R r = execute(lumongoConnection);
        long end = System.currentTimeMillis();
        r.setCommandTimeMs(end - start);
        return r;
    }
}
