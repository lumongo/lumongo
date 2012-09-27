package org.lumongo.client.pool;

import java.util.List;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.lumongo.client.command.Command;
import org.lumongo.client.config.LumongoClientConfig;
import org.lumongo.cluster.message.Lumongo.LMMember;

public class LumongoPool {
    private List<LMMember> members;
    private int retries;

    private GenericKeyedObjectPool<LMMember, LumongoConnection> connectionPool;

    public LumongoPool(LumongoClientConfig lumongoClientConfig) {
        members = lumongoClientConfig.getMembers();
        retries = lumongoClientConfig.getDefaultRetries();

        KeyedPoolableObjectFactory<LMMember, LumongoConnection> factory = new KeyedPoolableObjectFactory<LMMember, LumongoConnection>() {

            @Override
            public LumongoConnection makeObject(LMMember key) throws Exception {
                LumongoConnection lc = new LumongoConnection(key);
                lc.open();
                return lc;
            }

            @Override
            public void destroyObject(LMMember key, LumongoConnection obj) throws Exception {
                obj.close();
            }

            @Override
            public boolean validateObject(LMMember key, LumongoConnection obj) {
                return true;
            }

            @Override
            public void activateObject(LMMember key, LumongoConnection obj) throws Exception {
                // TODO maybe close the connection when idle
                // obj.close();
            }

            @Override
            public void passivateObject(LMMember key, LumongoConnection obj) throws Exception {
                // TODO maybe open the connection when not idle anymore
                // obj.open();
            }

        };
        GenericKeyedObjectPool.Config poolConfig = new GenericKeyedObjectPool.Config();
        // TODO from config
        poolConfig.maxIdle = 2;
        poolConfig.maxActive = 4;
        poolConfig.testOnBorrow = false;
        poolConfig.testOnReturn = false;

        connectionPool = new GenericKeyedObjectPool<LMMember, LumongoConnection>(factory, poolConfig);
    }

    public <R> R execute(Command<R> command) {

        // TODO use command retries if not null?
        for (int i = 0; i <= retries; i++) {
            LumongoConnection lumongoConnection = null;
            LMMember randomMember = null;
            try {
                int randomMemberIndex = (int) (Math.random() * members.size());
                randomMember = members.get(randomMemberIndex);

                lumongoConnection = connectionPool.borrowObject(randomMember);

                R r = command.execute(lumongoConnection);
                connectionPool.returnObject(randomMember, lumongoConnection);
                return r;
            }
            catch (Exception e) {
                if (randomMember != null && lumongoConnection != null) {
                    try {
                        connectionPool.invalidateObject(randomMember, lumongoConnection);
                    }
                    catch (Exception e1) {
                        // TODO do something?
                    }
                }
            }

        }

        return null;

    }

    public void close() throws Exception {
        connectionPool.close();
    }

}
