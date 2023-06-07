package org.streamprocessor.core.caches;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.streamprocessor.core.utils.CacheLoaderUtils;

public class SchemaCache {
    private static final Counter schemaCacheCallsCounter =
            Metrics.counter("SchemaCache", "schemaCacheCalls");
    private static final Counter schemaCacheMissesCounter =
            Metrics.counter("SchemaCache", "schemaCacheMisses");
    private static final LoadingCache<String, Schema> schemaCache =
            Caffeine.newBuilder()
                    .maximumSize(1000)
                    .refreshAfterWrite(5, TimeUnit.MINUTES)
                    .build(SchemaCache::loadSchemaToCache);

    private static Schema loadSchemaToCache(String ref) {
        schemaCacheMissesCounter.inc();
        return CacheLoaderUtils.getSchema(ref);
    }

    public static Schema getSchemaFromCache(String ref) {
        schemaCacheCallsCounter.inc();
        return schemaCache.get(ref);
    }
}
