package com.amazonaws.glue.catalog.metastore;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.glue.catalog.util.ConfMap;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

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

public final class AWSGlueClientFactory implements GlueClientFactory {

  private static final Logger logger = Logger.getLogger(AWSGlueClientFactory.class);

  private final ConfMap conf;

  public AWSGlueClientFactory(ConfMap conf) {
    Preconditions.checkNotNull(conf, "HiveConf cannot be null");
    this.conf = conf;
  }

  @Override
  public AWSGlue newClient() {
    try {
      AWSGlueClientBuilder glueClientBuilder = AWSGlueClientBuilder.standard()
          .withCredentials(getAWSCredentialsProvider(conf));

      String regionStr = getProperty(AWS_REGION, conf);
      String glueEndpoint = getProperty(AWS_GLUE_ENDPOINT, conf);

      // ClientBuilder only allows one of EndpointConfiguration or Region to be set
      if (StringUtils.isNotBlank(glueEndpoint)) {
        logger.info("Setting glue service endpoint to " + glueEndpoint);
        glueClientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(glueEndpoint, null));
      } else if (StringUtils.isNotBlank(regionStr)) {
        logger.info("Setting region to : " + regionStr);
        glueClientBuilder.setRegion(regionStr);
      } else {
        Region currentRegion = Regions.getCurrentRegion();
        if (currentRegion != null) {
          logger.info("Using region from ec2 metadata : " + currentRegion.getName());
          glueClientBuilder.setRegion(currentRegion.getName());
        } else {
          logger.info("No region info found, using SDK default region: us-east-1");
        }
      }

      glueClientBuilder.setClientConfiguration(buildClientConfiguration(conf));
      return glueClientBuilder.build();
    } catch (Exception e) {
      String message = "Unable to build AWSGlueClient: " + e;
      logger.error(message);
      throw e;
    }
  }

  private AWSCredentialsProvider getAWSCredentialsProvider(ConfMap conf) {
    Class<? extends AWSCredentialsProviderFactory> providerFactoryClass = conf
        .getClass(AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS,
            DefaultAWSCredentialsProviderFactory.class).asSubclass(
            AWSCredentialsProviderFactory.class);
    AWSCredentialsProviderFactory provider = ReflectionUtils.newInstance(
        providerFactoryClass, null);
    return provider.buildAWSCredentialsProvider(conf);
  }

  private ClientConfiguration buildClientConfiguration(ConfMap conf) {
    ClientConfiguration clientConfiguration = new ClientConfiguration()
        .withMaxErrorRetry(conf.getInt(AWS_GLUE_MAX_RETRY, DEFAULT_MAX_RETRY))
        .withMaxConnections(conf.getInt(AWS_GLUE_MAX_CONNECTIONS, DEFAULT_MAX_CONNECTIONS))
        .withConnectionTimeout(conf.getInt(AWS_GLUE_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT))
        .withSocketTimeout(conf.getInt(AWS_GLUE_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT));
    return clientConfiguration;
  }

  private static String getProperty(String propertyName, ConfMap conf) {
    return Strings.isNullOrEmpty(System.getProperty(propertyName)) ?
        conf.get(propertyName) : System.getProperty(propertyName);
  }
}
