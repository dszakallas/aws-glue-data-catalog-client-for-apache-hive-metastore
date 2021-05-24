package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.glue.catalog.util.ConfMap;
import com.amazonaws.services.glue.AWSGlue;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE;

public class AWSGlueMetastoreFactory {

    public AWSGlueMetastore newMetastore(ConfMap conf) {
        AWSGlue glueClient = new AWSGlueClientFactory(conf).newClient();
        AWSGlueMetastore defaultMetastore = new DefaultAWSGlueMetastore(conf, glueClient);
        if(isCacheEnabled(conf)) {
            return new AWSGlueMetastoreCacheDecorator(conf, defaultMetastore);
        }
        return defaultMetastore;
    }

    private boolean isCacheEnabled(ConfMap conf) {
        boolean databaseCacheEnabled = conf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        boolean tableCacheEnabled = conf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
        return (databaseCacheEnabled || tableCacheEnabled);
    }
}
