package org.lumongo.client.config;

public class LumongoPoolConfig extends LumongoClientConfig {

    private int maxConnections;
    private int maxIdle;

    public LumongoPoolConfig() {
        this.maxConnections = 8;
        this.maxIdle = 8;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

}
