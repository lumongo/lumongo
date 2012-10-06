package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;
import org.lumongo.client.result.Result;

import com.google.protobuf.ServiceException;

public abstract class Command<S, R extends Result> {

	public abstract S getRequest();

	public abstract R execute(LumongoConnection lumongoConnection) throws ServiceException;

	public R executeTimed(LumongoConnection lumongoConnection) throws ServiceException {
		long start = System.currentTimeMillis();
		R r = execute(lumongoConnection);
		long end = System.currentTimeMillis();
		r.setCommandTimeMs(end - start);
		return r;
	}
}
