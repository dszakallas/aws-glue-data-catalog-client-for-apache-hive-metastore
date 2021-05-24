package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.glue.catalog.util.ConfMap;

import com.amazonaws.auth.AWSCredentialsProvider;

public interface AWSCredentialsProviderFactory {

  AWSCredentialsProvider buildAWSCredentialsProvider(ConfMap conf);
}
