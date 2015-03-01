package org.lumongo.client.command.base;

import org.lumongo.client.LumongoRestClient;
import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.Result;

public abstract class RestCommand<R extends Result> extends Command<R> {

	public abstract R execute(LumongoRestClient lumongoRestClient) throws Exception;

	@Override
	public R execute(LumongoConnection lumongoConnection) throws Exception {
		return execute(lumongoConnection.getRestClient());
	}

}
