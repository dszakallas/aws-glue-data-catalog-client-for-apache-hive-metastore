package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.glue.catalog.util.ConfMap;
import com.amazonaws.services.glue.AWSGlue;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_CONNECTION_TIMEOUT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_ENDPOINT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_MAX_CONNECTIONS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_MAX_RETRY;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_GLUE_SOCKET_TIMEOUT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.AWS_REGION;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.DEFAULT_CONNECTION_TIMEOUT;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.DEFAULT_MAX_CONNECTIONS;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.DEFAULT_MAX_RETRY;
import static com.amazonaws.glue.catalog.util.AWSGlueConfig.DEFAULT_SOCKET_TIMEOUT;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AWSGlueClientFactoryTest {

  private static final String FAKE_ACCESS_KEY = "accessKey";
  private static final String FAKE_SECRET_KEY = "secretKey";
  private static final String FAKE_SESSION_TOKEN = "sessionToken";

  @Test
  public void testGlueClientConstructionWithHiveConfig() throws Exception {
    System.setProperty(AWS_REGION, "");
    System.setProperty(AWS_GLUE_ENDPOINT, "");

    ConfMap conf = spy(new ConfMap());
    GlueClientFactory glueClientFactory = new AWSGlueClientFactory(conf);

    when(conf.get(AWS_GLUE_ENDPOINT)).thenReturn("endpoint");
    when(conf.get(AWS_REGION)).thenReturn("us-west-1");

    AWSGlue glueClient = glueClientFactory.newClient();

    assertNotNull(glueClient);

    // client reads hive conf for region & endpoint
    verify(conf, atLeastOnce()).get(AWS_GLUE_ENDPOINT);
    verify(conf, atLeastOnce()).get(AWS_REGION);
  }

  @Test
  public void testGlueClientContructionWithAWSConfig() throws Exception {
    ConfMap conf = spy(new ConfMap());
    GlueClientFactory glueClientFactory = new AWSGlueClientFactory(conf);
    glueClientFactory.newClient();
    
    verify(conf, atLeastOnce()).getInt(AWS_GLUE_MAX_RETRY, DEFAULT_MAX_RETRY);
    verify(conf, atLeastOnce()).getInt(AWS_GLUE_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS);
    verify(conf, atLeastOnce()).getInt(AWS_GLUE_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
    verify(conf, atLeastOnce()).getInt(AWS_GLUE_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
  }

  @Test
  public void testGlueClientConstructionWithSystemProperty() throws Exception {
    System.setProperty(AWS_REGION, "us-east-1");
    System.setProperty(AWS_GLUE_ENDPOINT, "endpoint");
    ConfMap conf = spy(new ConfMap());
    GlueClientFactory glueClientFactory = new AWSGlueClientFactory(conf);
    AWSGlue glueClient = glueClientFactory.newClient();

    assertNotNull(glueClient);

    // client has no interactions with the hive conf since system property is set
    verify(conf, never()).get(AWS_GLUE_ENDPOINT);
    verify(conf, never()).get(AWS_REGION);
  }

  @Test
  public void testClientConstructionWithSessionCredentialsProviderFactory() throws Exception {
    System.setProperty("aws.region", "us-west-2");

    Map<String, String> props = new HashMap<>();
    props.put(SessionCredentialsProviderFactory.AWS_ACCESS_KEY_CONF_VAR, FAKE_ACCESS_KEY);
    props.put(SessionCredentialsProviderFactory.AWS_SECRET_KEY_CONF_VAR, FAKE_SECRET_KEY);
    props.put(SessionCredentialsProviderFactory.AWS_SESSION_TOKEN_CONF_VAR, FAKE_SESSION_TOKEN);

    props.put(AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS,
            SessionCredentialsProviderFactory.class.getCanonicalName());
    
    ConfMap conf = spy(new ConfMap(props));
    GlueClientFactory glueClientFactory = new AWSGlueClientFactory(conf);

    AWSGlue glueClient = glueClientFactory.newClient();

    assertNotNull(glueClient);

    verify(conf, atLeastOnce()).get(SessionCredentialsProviderFactory.AWS_ACCESS_KEY_CONF_VAR);
    verify(conf, atLeastOnce()).get(SessionCredentialsProviderFactory.AWS_SECRET_KEY_CONF_VAR);
    verify(conf, atLeastOnce()).get(SessionCredentialsProviderFactory.AWS_SESSION_TOKEN_CONF_VAR);
  }

  @Test
  public void testCredentialsCreatedBySessionCredentialsProviderFactory() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(SessionCredentialsProviderFactory.AWS_ACCESS_KEY_CONF_VAR, FAKE_ACCESS_KEY);
    props.put(SessionCredentialsProviderFactory.AWS_SECRET_KEY_CONF_VAR, FAKE_SECRET_KEY);
    props.put(SessionCredentialsProviderFactory.AWS_SESSION_TOKEN_CONF_VAR, FAKE_SESSION_TOKEN);

    ConfMap conf = spy(new ConfMap(props));

    SessionCredentialsProviderFactory factory = new SessionCredentialsProviderFactory();
    AWSCredentialsProvider provider = factory.buildAWSCredentialsProvider(conf);
    AWSCredentials credentials = provider.getCredentials();

    assertThat(credentials, instanceOf(BasicSessionCredentials.class));

    BasicSessionCredentials sessionCredentials = (BasicSessionCredentials) credentials;

    assertEquals(FAKE_ACCESS_KEY, sessionCredentials.getAWSAccessKeyId());
    assertEquals(FAKE_SECRET_KEY, sessionCredentials.getAWSSecretKey());
    assertEquals(FAKE_SESSION_TOKEN, sessionCredentials.getSessionToken());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingAccessKeyWithSessionCredentialsProviderFactory() throws Exception {
    ConfMap conf = spy(new ConfMap());
    SessionCredentialsProviderFactory factory = new SessionCredentialsProviderFactory();
    factory.buildAWSCredentialsProvider(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingSecretKey() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(SessionCredentialsProviderFactory.AWS_ACCESS_KEY_CONF_VAR, FAKE_ACCESS_KEY);
    ConfMap conf = spy(new ConfMap(props));

    SessionCredentialsProviderFactory factory = new SessionCredentialsProviderFactory();

    factory.buildAWSCredentialsProvider(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingSessionTokenKey() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(SessionCredentialsProviderFactory.AWS_ACCESS_KEY_CONF_VAR, FAKE_ACCESS_KEY);
    props.put(SessionCredentialsProviderFactory.AWS_SECRET_KEY_CONF_VAR, FAKE_SECRET_KEY);
    ConfMap conf = spy(new ConfMap(props));

    SessionCredentialsProviderFactory factory = new SessionCredentialsProviderFactory();

    factory.buildAWSCredentialsProvider(conf);
  }

}
