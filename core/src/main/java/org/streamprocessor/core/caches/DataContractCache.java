package org.streamprocessor.core.caches;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.json.JSONObject;
import org.streamprocessor.core.utils.CacheLoaderUtils;

public class DataContractCache {
    private static final Counter dataContractCacheCallsCounter =
            Metrics.counter("DataContractCache", "dataContractCacheCallsCounter");
    private static final Counter dataContractsCacheMissesCounter =
            Metrics.counter("DataContractCache", "dataContractsCacheMissesCounter");
    private static final LoadingCache<String, JSONObject> dataContractCache =
            Caffeine.newBuilder()
                    .maximumSize(1000)
                    .refreshAfterWrite(5, TimeUnit.MINUTES)
                    .build(k -> loadDataContractToCache(k));

    private static JSONObject loadDataContractToCache(String endpoint) {
        dataContractsCacheMissesCounter.inc();
        return CacheLoaderUtils.getDataContract(endpoint);
    }

    public static LoadingCache<String, JSONObject> getDataContractFromCache() {
        dataContractCacheCallsCounter.inc();
        return dataContractCache;
    }
}
