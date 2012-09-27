package org.lumongo.client.pool;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.lumongo.util.LumongoThreadFactory;

public class WorkPool {

    private ThreadPoolExecutor pool;
    private final static AtomicInteger threadNumber = new AtomicInteger(1);

    public WorkPool(int threads) {
        this(threads, threads * 2);
    }

    public WorkPool(int threads, int maxQueued) {
        this(threads, maxQueued, "workPool-" + threadNumber.getAndIncrement());
    }

    public WorkPool(int threads, int maxQueued, String poolName) {
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(maxQueued) {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean offer(Runnable e) {
                try {
                    put(e);
                }
                catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
                return true;
            };
        };

        pool = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS, workQueue, new LumongoThreadFactory(poolName));
    }

    public <T> Future<T> executeAsync(Callable<T> task) {
        return pool.submit(task);
    }

    public void shutdown() throws Exception {
        pool.shutdown();
        boolean terminated = false;
        try {
            while (!terminated) {
                // terminates immediately on completion
                terminated = pool.awaitTermination(1, TimeUnit.HOURS);
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
