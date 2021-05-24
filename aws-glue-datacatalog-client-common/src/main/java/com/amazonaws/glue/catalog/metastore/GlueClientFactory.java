package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.services.glue.AWSGlue;

/***
 * Interface for creating Glue AWS Client
 */
public interface GlueClientFactory {

  AWSGlue newClient();

}
