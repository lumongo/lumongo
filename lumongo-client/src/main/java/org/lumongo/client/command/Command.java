package org.lumongo.client.command;

import org.lumongo.client.pool.LumongoConnection;

import com.google.protobuf.ServiceException;

public abstract class Command<S, R> {

    public abstract S getRequest();

    public abstract R execute(LumongoConnection lumongoConnection) throws ServiceException;
}
