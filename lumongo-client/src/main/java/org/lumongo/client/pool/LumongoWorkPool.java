package org.lumongo.client.pool;

import org.lumongo.client.command.GetMembers;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.result.GetMembersResult;

public class LumongoWorkPool extends LumongoBaseWorkPool {

    public LumongoWorkPool(LumongoPoolConfig lumongoPoolConfig) {
        super(lumongoPoolConfig);
    }

    public void updateMembers() throws Exception {
        GetMembersResult getMembersResult = execute(new GetMembers());
        updateMembers(getMembersResult.getMembers());
    }
}
