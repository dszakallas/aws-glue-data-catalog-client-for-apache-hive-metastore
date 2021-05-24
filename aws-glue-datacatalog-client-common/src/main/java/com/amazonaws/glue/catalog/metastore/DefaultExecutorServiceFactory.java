package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.glue.catalog.util.ConfMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultExecutorServiceFactory implements ExecutorServiceFactory {
    private static final int NUM_EXECUTOR_THREADS = 5;

    static final String GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT = "glue-metastore-delegate-%d";

    private static final ExecutorService GLUE_METASTORE_DELEGATE_THREAD_POOL = Executors.newFixedThreadPool(
      NUM_EXECUTOR_THREADS, new ThreadFactoryBuilder()
        .setNameFormat(GLUE_METASTORE_DELEGATE_THREADPOOL_NAME_FORMAT)
        .setDaemon(true).build()
    );

    @Override
    public ExecutorService getExecutorService(ConfMap conf) {
        return GLUE_METASTORE_DELEGATE_THREAD_POOL;
    }
}
