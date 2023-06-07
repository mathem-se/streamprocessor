package org.streamprocessor.core.caches;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.json.JSONObject;
import org.streamprocessor.core.utils.CacheLoaderUtils;

public class DataContractsCache {
    private static final Counter dataContractsCacheCallsCounter =
            Metrics.counter("DataContractsCache", "dataContractsCacheCalls");
    private static final Counter dataContractsCacheMissesCounter =
            Metrics.counter("DataContractsCache", "dataContractsCacheMisses");
    private static final LoadingCache<String, JSONObject> dataContractsCache =
            Caffeine.newBuilder()
                    .maximumSize(1000)
                    .refreshAfterWrite(5, TimeUnit.MINUTES)
                    .build(DataContractsCache::loadDataContractToCache);

    private static JSONObject loadDataContractToCache(String endpoint) {
        dataContractsCacheMissesCounter.inc();
        return CacheLoaderUtils.getDataContract(endpoint);
    }

    public static JSONObject getDataContractFromCache(String endpoint) {
        dataContractsCacheCallsCounter.inc();
        return dataContractsCache.get(endpoint);
    }
}
