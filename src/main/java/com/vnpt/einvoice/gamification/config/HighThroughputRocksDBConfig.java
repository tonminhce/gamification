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
 * Bounds read cache globally while letting each store size its own memtables without cross-DB
 * flush storms on small window stores.
 */
public class HighThroughputRocksDBConfig implements RocksDBConfigSetter {

    public static final String BLOCK_CACHE_MB_CONFIG = "app.rocksdb.block-cache-mb";
    public static final String WRITE_BUFFER_MB_CONFIG = "app.rocksdb.write-buffer-mb";

    private static final long DEFAULT_BLOCK_CACHE_BYTES = 256L * 1024L * 1024L;

    private static volatile Cache sharedBlockCache;
    private static final Object INIT_LOCK = new Object();

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        ensureSharedBlockCache(configs);

        BlockBasedTableConfig tableConfig = options.tableFormatConfig() instanceof BlockBasedTableConfig existing
                ? existing
                : new BlockBasedTableConfig();
        tableConfig.setBlockCache(sharedBlockCache);
        tableConfig.setBlockSize(16L * 1024L);
        tableConfig.setFilterPolicy(new BloomFilter(10.0, false));
        tableConfig.setCacheIndexAndFilterBlocks(true);
        tableConfig.setPinL0FilterAndIndexBlocksInCache(true);

        options.setTableFormatConfig(tableConfig);
        options.setWriteBufferSize(16L * 1024L * 1024L);
        options.setMaxWriteBufferNumber(3);
        options.setMinWriteBufferNumberToMerge(1);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);
        options.setCompactionStyle(CompactionStyle.LEVEL);
        options.setMaxBackgroundJobs(4);
    }

    @Override
    public void close(String storeName, Options options) {
        // Shared block cache lives across store lifecycles.
    }

    private static void ensureSharedBlockCache(Map<String, Object> configs) {
        if (sharedBlockCache != null) {
            return;
        }
        synchronized (INIT_LOCK) {
            if (sharedBlockCache == null) {
                long cacheBytes = parseMb(configs, BLOCK_CACHE_MB_CONFIG, DEFAULT_BLOCK_CACHE_BYTES);
                sharedBlockCache = new LRUCache(cacheBytes);
            }
        }
    }

    private static long parseMb(Map<String, Object> configs, String key, long defaultBytes) {
        if (configs == null) {
            return defaultBytes;
        }
        Object raw = configs.get(key);
        if (raw instanceof Number number) {
            return Math.max(16L, number.longValue()) * 1024L * 1024L;
        }
        if (raw instanceof String text && !text.isBlank()) {
            try {
                return Math.max(16L, Long.parseLong(text.trim())) * 1024L * 1024L;
            } catch (NumberFormatException ignored) {
                return defaultBytes;
            }
        }
        return defaultBytes;
    }
}
