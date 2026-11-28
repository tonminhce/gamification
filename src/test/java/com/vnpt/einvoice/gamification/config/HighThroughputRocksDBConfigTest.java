package com.vnpt.einvoice.gamification.config;

import org.junit.jupiter.api.Test;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class HighThroughputRocksDBConfigTest {

    @Test
    void configuresSharedCacheBloomAndWriteBufferAndClosesCleanly() {
        HighThroughputRocksDBConfig setter = new HighThroughputRocksDBConfig();
        Options options = new Options();
        try {
            setter.setConfig("test-store", options, Map.of(
                    HighThroughputRocksDBConfig.BLOCK_CACHE_MB_CONFIG, 32L,
                    HighThroughputRocksDBConfig.WRITE_BUFFER_MB_CONFIG, 8L
            ));

            assertEquals(8L * 1024L * 1024L, options.writeBufferSize());
            assertEquals(CompressionType.LZ4_COMPRESSION, options.compressionType());
            assertInstanceOf(BlockBasedTableConfig.class, options.tableFormatConfig());
        } finally {
            setter.close("test-store", options);
            options.close();
        }
    }

    @Test
    void concurrentStoresSharingSameLogicalNameDoNotFreeEachOthersFilters() throws Exception {
        int workers = 12;
        CyclicBarrier barrier = new CyclicBarrier(workers);
        try (var pool = Executors.newFixedThreadPool(workers)) {
            List<Future<?>> futures = new ArrayList<>(workers);
            for (int i = 0; i < workers; i++) {
                futures.add(pool.submit(() -> {
                    HighThroughputRocksDBConfig setter = new HighThroughputRocksDBConfig();
                    Options options = new Options();
                    try {
                        barrier.await();
                        setter.setConfig("company-window-store", options, Map.of(
                                HighThroughputRocksDBConfig.BLOCK_CACHE_MB_CONFIG, 32L
                        ));
                    } finally {
                        setter.close("company-window-store", options);
                        options.close();
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        }
    }
}
