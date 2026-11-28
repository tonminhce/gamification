package com.vnpt.einvoice.gamification.config;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;

import java.util.Map;

/**
 * Shared off-heap block cache and SST Bloom filter tuning for Kafka Streams RocksDB stores.
 * Each setter instance owns the native {@link BloomFilter} handle for its store/segment while
 * ref-counting the shared {@link LRUCache} across all open RocksDB instances.
 */
public class HighThroughputRocksDBConfig implements RocksDBConfigSetter {

    public static final String BLOCK_CACHE_MB_CONFIG = "app.rocksdb.block-cache-mb";
    public static final String WRITE_BUFFER_MB_CONFIG = "app.rocksdb.write-buffer-mb";

    private static final long DEFAULT_BLOCK_CACHE_BYTES = 256L * 1024L * 1024L;
    private static final long DEFAULT_WRITE_BUFFER_BYTES = 16L * 1024L * 1024L;

    private static final Object CACHE_LOCK = new Object();
    private static Cache sharedBlockCache;
    private static long sharedCacheBytes = -1L;
    private static int activeStores = 0;

    private BloomFilter filterPolicy;
    private boolean registeredInCache;

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        long requestedCacheBytes = parseMb(configs, BLOCK_CACHE_MB_CONFIG, DEFAULT_BLOCK_CACHE_BYTES);
        long writeBufferBytes = parseMb(configs, WRITE_BUFFER_MB_CONFIG, DEFAULT_WRITE_BUFFER_BYTES);
        Cache blockCache = acquireSharedBlockCache(requestedCacheBytes);

        if (this.filterPolicy != null) {
            this.filterPolicy.close();
        }
        this.filterPolicy = new BloomFilter(10.0, false);

        BlockBasedTableConfig tableConfig = options.tableFormatConfig() instanceof BlockBasedTableConfig existing
                ? existing
                : new BlockBasedTableConfig();
        tableConfig.setBlockCache(blockCache);
        tableConfig.setBlockSize(16L * 1024L);
        tableConfig.setFilterPolicy(this.filterPolicy);
        tableConfig.setCacheIndexAndFilterBlocks(true);
        tableConfig.setPinL0FilterAndIndexBlocksInCache(true);

        options.setTableFormatConfig(tableConfig);
        options.setWriteBufferSize(writeBufferBytes);
        options.setMaxWriteBufferNumber(3);
        options.setMinWriteBufferNumberToMerge(1);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);
        options.setCompactionStyle(CompactionStyle.LEVEL);
        options.setMaxBackgroundJobs(4);
    }

    @Override
    public void close(String storeName, Options options) {
        if (this.filterPolicy != null) {
            this.filterPolicy.close();
            this.filterPolicy = null;
        }
        releaseSharedBlockCacheIfUnused();
    }

    private Cache acquireSharedBlockCache(long requestedBytes) {
        synchronized (CACHE_LOCK) {
            if (!registeredInCache) {
                activeStores++;
                registeredInCache = true;
            }
            if (sharedBlockCache == null) {
                sharedBlockCache = new LRUCache(requestedBytes);
                sharedCacheBytes = requestedBytes;
            } else if (sharedCacheBytes != requestedBytes && activeStores == 1) {
                sharedBlockCache.close();
                sharedBlockCache = new LRUCache(requestedBytes);
                sharedCacheBytes = requestedBytes;
            }
            return sharedBlockCache;
        }
    }

    private void releaseSharedBlockCacheIfUnused() {
        synchronized (CACHE_LOCK) {
            if (!registeredInCache) {
                return;
            }
            registeredInCache = false;
            activeStores = Math.max(0, activeStores - 1);
            if (activeStores == 0 && sharedBlockCache != null) {
                sharedBlockCache.close();
                sharedBlockCache = null;
                sharedCacheBytes = -1L;
            }
        }
    }

    private static long parseMb(Map<String, Object> configs, String key, long defaultBytes) {
        if (configs == null) {
            return defaultBytes;
        }
        Object raw = configs.get(key);
        if (raw instanceof Number number) {
            return Math.max(4L, number.longValue()) * 1024L * 1024L;
        }
        if (raw instanceof String text && !text.isBlank()) {
            try {
                return Math.max(4L, Long.parseLong(text.trim())) * 1024L * 1024L;
            } catch (NumberFormatException ignored) {
                return defaultBytes;
            }
        }
        return defaultBytes;
    }
}
