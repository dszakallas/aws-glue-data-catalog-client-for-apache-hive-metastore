package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.glue.catalog.util.ConfMap;
import org.junit.Before;
import org.junit.Test;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_ENABLE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_ENDPOINT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_TABLE_CACHE_TTL_MINS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_SIZE;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_DB_CACHE_TTL_MINS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_REGION;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertTrue;

public class AWSGlueMetastoreFactoryTest {

    private AWSGlueMetastoreFactory awsGlueMetastoreFactory;
    private ConfMap conf;

    @Before
    public void setUp() {
        awsGlueMetastoreFactory = new AWSGlueMetastoreFactory();
        conf = spy(new ConfMap());

        // these configs are needed for AWSGlueClient to get initialized
        System.setProperty(AWS_REGION, "");
        System.setProperty(AWS_GLUE_ENDPOINT, "");
        when(conf.get(AWS_GLUE_ENDPOINT)).thenReturn("endpoint");
        when(conf.get(AWS_REGION)).thenReturn("us-west-1");

        // these configs are needed for AWSGlueMetastoreCacheDecorator to get initialized
        when(conf.getInt(AWS_GLUE_DB_CACHE_SIZE, 0)).thenReturn(1);
        when(conf.getInt(AWS_GLUE_DB_CACHE_TTL_MINS, 0)).thenReturn(1);
        when(conf.getInt(AWS_GLUE_TABLE_CACHE_SIZE, 0)).thenReturn(1);
        when(conf.getInt(AWS_GLUE_TABLE_CACHE_TTL_MINS, 0)).thenReturn(1);
    }

    @Test
    public void testNewMetastoreWhenCacheDisabled() throws Exception {
        when(conf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
        when(conf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(false);
        assertTrue(DefaultAWSGlueMetastore.class.equals(
                awsGlueMetastoreFactory.newMetastore(conf).getClass()));
        verify(conf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        verify(conf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
    }

    @Test
    public void testNewMetastoreWhenTableCacheEnabled() throws Exception {
        when(conf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(false);
        when(conf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(true);
        assertTrue(AWSGlueMetastoreCacheDecorator.class.equals(
                awsGlueMetastoreFactory.newMetastore(conf).getClass()));
        verify(conf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        verify(conf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
    }

    @Test
    public void testNewMetastoreWhenDBCacheEnabled() throws Exception {
        when(conf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(true);
        when(conf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(true);
        assertTrue(AWSGlueMetastoreCacheDecorator.class.equals(
                awsGlueMetastoreFactory.newMetastore(conf).getClass()));
        verify(conf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        verify(conf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
    }

    @Test
    public void testNewMetastoreWhenAllCacheEnabled() throws Exception {
        when(conf.getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false)).thenReturn(true);
        when(conf.getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false)).thenReturn(true);
        assertTrue(AWSGlueMetastoreCacheDecorator.class.equals(
                awsGlueMetastoreFactory.newMetastore(conf).getClass()));
        verify(conf, atLeastOnce()).getBoolean(AWS_GLUE_DB_CACHE_ENABLE, false);
        verify(conf, atLeastOnce()).getBoolean(AWS_GLUE_TABLE_CACHE_ENABLE, false);
    }

}