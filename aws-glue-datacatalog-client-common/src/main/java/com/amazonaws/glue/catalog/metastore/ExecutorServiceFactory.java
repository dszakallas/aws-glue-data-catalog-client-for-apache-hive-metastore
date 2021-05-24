package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.glue.catalog.util.ConfMap;

import java.util.concurrent.ExecutorService;

/*
 * Interface for creating an ExecutorService
 */
public interface ExecutorServiceFactory {
    public ExecutorService getExecutorService(ConfMap conf);
}
