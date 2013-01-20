package org.lumongo.client.pool;

import java.util.List;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.lumongo.client.command.GetMembers;
import org.lumongo.client.command.base.Command;
import org.lumongo.client.config.LumongoPoolConfig;
import org.lumongo.client.result.GetMembersResult;
import org.lumongo.client.result.Result;
import org.lumongo.cluster.message.Lumongo.LMMember;

public class LumongoPool {
    private List<LMMember> members;
    private int retries;
    private int maxIdle;
    private int maxConnections;


    private GenericKeyedObjectPool<LMMember, LumongoConnection> connectionPool;

    public LumongoPool(final LumongoPoolConfig lumongoPoolConfig) {
        members = lumongoPoolConfig.getMembers();
        retries = lumongoPoolConfig.getDefaultRetries();
        maxIdle = lumongoPoolConfig.getMaxIdle();
        maxConnections = lumongoPoolConfig.getMaxConnections();

        KeyedPoolableObjectFactory<LMMember, LumongoConnection> factory = new KeyedPoolableObjectFactory<LMMember, LumongoConnection>() {

            @Override
            public LumongoConnection makeObject(LMMember key) throws Exception {
                LumongoConnection lc = new LumongoConnection(key);
                lc.open(lumongoPoolConfig.isCompressedConnection());
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

            }

            @Override
            public void passivateObject(LMMember key, LumongoConnection obj) throws Exception {

            }

        };
        GenericKeyedObjectPool.Config poolConfig = new GenericKeyedObjectPool.Config(); //
        poolConfig.maxIdle = maxIdle;
        poolConfig.maxActive = maxConnections;

        poolConfig.testOnBorrow = false;
        poolConfig.testOnReturn = false;

        connectionPool = new GenericKeyedObjectPool<LMMember, LumongoConnection>(factory, poolConfig);
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void updateMembers(List<LMMember> members) {
        //TODO handle cleaning up out of the pool?
        this.members = members;
    }

    public void updateMembers() throws Exception {
        GetMembersResult getMembersResult = execute(new GetMembers());
        updateMembers(getMembersResult.getMembers());
    }

    public <R extends Result> R execute(Command<R> command) throws Exception {


        int tries = 0;
        while (true) {
            LumongoConnection lumongoConnection = null;
            LMMember randomMember = null;
            try {
                List<LMMember> tempList = members; //stop array index out bounds on updates without locking
                int randomMemberIndex = (int) (Math.random() * tempList.size());
                randomMember = tempList.get(randomMemberIndex);

                lumongoConnection = connectionPool.borrowObject(randomMember);

                R r = command.executeTimed(lumongoConnection);

                connectionPool.returnObject(randomMember, lumongoConnection);
                return r;
            }
            catch (Exception e) {
                if (randomMember != null && lumongoConnection != null) {
                    try {
                        connectionPool.invalidateObject(randomMember, lumongoConnection);
                    }
                    catch (Exception e1) {
                    }
                }
                if (tries >= retries) {
                    throw e;
                }
                tries++;
            }
        }


    }

    public void close() throws Exception {
        connectionPool.close();
    }

}
