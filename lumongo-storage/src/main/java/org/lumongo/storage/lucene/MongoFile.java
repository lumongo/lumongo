package org.lumongo.storage.lucene;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.lumongo.util.Compression;
import org.lumongo.util.Compression.CompressionLevel;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

public class MongoFile implements NosqlFile {

    private final MongoDirectory mongoDirectory;
    private final String fileName;
    private final int fileNumber;
    private final int blockSize;

    private long fileLength;
    private long lastModified;

    private MongoBlock currentReadBlock;
    private MongoBlock currentWriteBlock;

    private ConcurrentMap<Integer, Lock> blockLocks;

    private Cache<Integer, MongoBlock> dirtyBlocks;
    private LoadingCache<Integer, MongoBlock> cache;
    private boolean compressed;

    protected MongoFile(MongoDirectory mongoDirectory, String fileName, int fileNumber, int blockSize, boolean compressed) {

        this.mongoDirectory = mongoDirectory;
        this.fileName = fileName;
        this.fileNumber = fileNumber;
        this.fileLength = 0;
        this.lastModified = System.currentTimeMillis();

        this.blockSize = blockSize;
        this.compressed = compressed;

        this.blockLocks = new ConcurrentHashMap<Integer, Lock>();

        RemovalListener<Integer, MongoBlock> removalListener = new RemovalListener<Integer, MongoBlock>() {

            @Override
            public void onRemoval(RemovalNotification<Integer, MongoBlock> notification) {
                if (RemovalCause.EXPLICIT.equals(notification.getCause())) {
                    Integer key = notification.getKey();
                    Lock l = blockLocks.get(key);
                    l.lock();
                    try {

                        MongoBlock mb = notification.getValue();

                        if (mb.dirty) {
                            storeBlock(mb);
                        }

                    }
                    finally {
                        l.unlock();
                    }
                }

            }
        };

        CacheLoader<Integer, MongoBlock> cacheLoader = new CacheLoader<Integer, MongoBlock>() {
            @Override
            public MongoBlock load(Integer key) throws Exception {
                if (!blockLocks.containsKey(key)) {
                    blockLocks.putIfAbsent(key, new ReentrantLock());
                }

                Lock l = blockLocks.get(key);
                l.lock();
                try {
                    MongoBlock mb = dirtyBlocks.getIfPresent(key);
                    if (mb != null) {
                        return mb;
                    }
                    mb = getBlock(key, true);
                    MongoDirectory.cacheBlock(mb);
                    return mb;
                }
                catch (Exception e) {
                    System.err.println("Exception in mongo block <" + key + "> for file <" + MongoFile.this + ">:" + e);
                    e.printStackTrace();
                    throw e;
                }
                finally {
                    l.unlock();
                }
            }
        };

        this.dirtyBlocks = CacheBuilder.newBuilder().removalListener(removalListener).softValues().build();

        this.cache = CacheBuilder.newBuilder().softValues().build(cacheLoader);

    }

    public MongoDirectory getMongoDirectory() {
        return mongoDirectory;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public long getFileLength() {

        return fileLength;
    }

    @Override
    public void setFileLength(long fileLength) {
        this.fileLength = fileLength;
    }

    @Override
    public long getLastModified() {
        return lastModified;
    }

    @Override
    public void setLastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    @Override
    public byte readByte(long position) throws IOException {
        try {
            // System.out.println("Read " + filename + ": position: " + position + " length: 1");
            int block = (int) (position / mongoDirectory.getBlockSize());
            int blockOffset = (int) (position - (block * mongoDirectory.getBlockSize()));

            MongoBlock mb = currentReadBlock;

            if (mb == null || block != mb.blockNumber) {
                currentReadBlock = mb = cache.get(block);
            }

            return mb.bytes[blockOffset];
        }
        catch (ExecutionException e) {
            throw new IOException("Failed to read byte at position: " + position);
        }
    }

    @Override
    public void readBytes(long position, byte[] b, int offset, int length) throws IOException {
        try {
            // System.out.println("Read " + filename + ": position: " + position + " length: " + length);

            while (length > 0) {
                int block = (int) (position / blockSize);
                int blockOffset = (int) (position - (block * blockSize));

                int readSize = Math.min(blockSize - blockOffset, length);

                MongoBlock mb = currentReadBlock;

                if (mb == null || block != mb.blockNumber) {
                    currentReadBlock = mb = cache.get(block);
                }

                System.arraycopy(mb.bytes, blockOffset, b, offset, readSize);

                position += readSize;
                offset += readSize;
                length -= readSize;
            }
        }
        catch (ExecutionException e) {
            throw new IOException("Failed to read bytes at position: " + position);
        }
    }

    @Override
    public void write(long position, byte b) throws IOException {
        try {
            int block = (int) (position / blockSize);
            int blockOffset = (int) (position - (block * blockSize));

            // System.out.println("Write " + filename + ": position: " + position + " length: 1 block:" + block);

            MongoBlock mb = currentWriteBlock;

            if (mb == null || block != mb.blockNumber) {
                if (mb != null) {
                    markDirty(mb);
                }
                currentWriteBlock = mb = cache.get(block);
            }

            mb.bytes[blockOffset] = b;
            mb.dirty = true;

            fileLength = Math.max(position + 1, fileLength);
        }
        catch (ExecutionException e) {
            throw new IOException("Failed to write byte at position: " + position);
        }
    }

    @Override
    public void write(long position, byte[] b, int offset, int length) throws IOException {
        try {
            // System.out.println("Write " + filename + ": position: " + position + " length: " + length);
            while (length > 0) {
                int block = (int) (position / blockSize);
                int blockOffset = (int) (position - (block * blockSize));
                int writeSize = Math.min(blockSize - blockOffset, length);

                MongoBlock mb = currentWriteBlock;

                if (mb == null || block != mb.blockNumber) {
                    if (mb != null) {
                        markDirty(mb);
                    }
                    currentWriteBlock = mb = cache.get(block);
                }

                System.arraycopy(b, offset, mb.bytes, blockOffset, writeSize);
                mb.dirty = true;
                position += writeSize;
                offset += writeSize;
                length -= writeSize;
            }
            fileLength = Math.max(position + length, fileLength);
        }
        catch (ExecutionException e) {
            throw new IOException("Failed to write bytes at position: " + position);
        }
    }

    private void markDirty(MongoBlock mb) {
        int blockNumber = mb.blockNumber;
        Lock l = blockLocks.get(blockNumber);
        l.lock();
        try {
            MongoDirectory.cacheBlock(mb);
            dirtyBlocks.put(blockNumber, mb);
        }
        finally {
            l.unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        // System.out.println("Flush");

        if (currentWriteBlock != null) {
            dirtyBlocks.put(currentWriteBlock.blockNumber, currentWriteBlock);
        }

        Set<Integer> dirtyBlockKeys = new HashSet<Integer>(dirtyBlocks.asMap().keySet());

        dirtyBlocks.invalidateAll(dirtyBlockKeys);

        mongoDirectory.updateFileMetadata(this);
    }

    private MongoBlock getBlock(Integer blockNumber, boolean createIfNotExist) throws IOException {

        DBCollection c = mongoDirectory.getBlocksCollection();

        DBObject query = new BasicDBObject();
        query.put(MongoDirectory.FILE_NUMBER, fileNumber);
        query.put(MongoDirectory.BLOCK_NUMBER, blockNumber);

        DBObject result = c.findOne(query);

        byte[] bytes = null;
        if (result != null) {
            // System.out.println("Fetch: filename:" + fileName + " block: " + blockNumber);
            bytes = (byte[]) result.get(MongoDirectory.BYTES);
            boolean blockCompressed = (boolean) result.get(MongoDirectory.COMPRESSED);
            if (blockCompressed) {
                bytes = Compression.uncompressZlib(bytes);
                // bytes = Snappy.uncompress(bytes);
            }

            return new MongoBlock(this, fileNumber, blockNumber, bytes);
        }

        if (createIfNotExist) {
            // System.out.println("Create: filename:" + fileName + " block: " + blockNumber);
            bytes = new byte[blockSize];
            MongoBlock mongoBlock = new MongoBlock(this, fileNumber, blockNumber, bytes);
            storeBlock(mongoBlock);
            return mongoBlock;
        }

        return null;

    }

    public void storeBlock(MongoBlock mongoBlock) {
        // System.out.println("Store: " + mongoBlock.getBlockNumber());

        DBCollection c = mongoDirectory.getBlocksCollection();

        DBObject query = new BasicDBObject();
        query.put(MongoDirectory.FILE_NUMBER, fileNumber);
        query.put(MongoDirectory.BLOCK_NUMBER, mongoBlock.blockNumber);

        DBObject object = new BasicDBObject();
        object.put(MongoDirectory.FILE_NUMBER, fileNumber);
        object.put(MongoDirectory.BLOCK_NUMBER, mongoBlock.blockNumber);
        byte[] orgBytes = mongoBlock.bytes;
        byte[] newBytes = orgBytes;
        boolean blockCompressed = compressed;
        if (blockCompressed) {
            try {
                newBytes = Compression.compressZlib(orgBytes, CompressionLevel.BEST);
                if (newBytes.length >= orgBytes.length) {
                    System.out.println("Disabling compression for block <" + mongoBlock + "> compresion size <" + newBytes.length
                            + "> greater than or equals to old size <" + orgBytes.length + ">");
                    newBytes = orgBytes;
                    blockCompressed = false;
                }
            }
            catch (Exception e) {
                System.err.println("Failed to compress block : <" + mongoBlock + ">.  Storing uncompressed: " + e);
                newBytes = orgBytes;
                blockCompressed = false;
            }
        }
        object.put(MongoDirectory.BYTES, newBytes);
        object.put(MongoDirectory.COMPRESSED, blockCompressed);

        c.update(query, object, true, false, WriteConcern.SAFE);
        mongoBlock.dirty = false;

    }

    @Override
    public String toString() {
        return "MongoFile [fileName=" + fileName + ", fileNumber=" + fileNumber + ", blockSize=" + blockSize + ", fileLength=" + fileLength + ", lastModified="
                + lastModified + ", compressed=" + compressed + "]";
    }

    @Override
    public int getFileNumber() {
        return fileNumber;
    }

    @Override
    public int getBlockSize() {
        return blockSize;
    }

    @Override
    public boolean isCompressed() {
        return compressed;
    }
}
