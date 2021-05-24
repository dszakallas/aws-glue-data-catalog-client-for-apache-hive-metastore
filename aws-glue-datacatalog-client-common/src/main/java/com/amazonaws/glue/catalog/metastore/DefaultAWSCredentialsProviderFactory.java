package com.amazonaws.glue.catalog.metastore;


import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.glue.catalog.util.ConfMap;

public class DefaultAWSCredentialsProviderFactory implements
    AWSCredentialsProviderFactory {

  @Override
  public AWSCredentialsProvider buildAWSCredentialsProvider(ConfMap conf) {
    return new DefaultAWSCredentialsProviderChain();
  }

}
