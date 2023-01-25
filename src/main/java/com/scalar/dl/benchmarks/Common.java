package com.scalar.dl.benchmarks;

import com.scalar.dl.client.config.ClientConfig;
import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.exception.IllegalConfigException;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Common {
  private static final Logger LOGGER = LoggerFactory.getLogger(Common.class);
  private static final String CONFIG_NAME = "client_config";
  private static final String LEDGER_HOST = "ledger_host";
  private static final String LEDGER_PORT = "ledger_port";
  private static final String AUDITOR_HOST = "auditor_host";
  private static final String AUDITOR_PORT = "auditor_port";
  private static final String AUDITOR_ENABLED = "auditor_enabled";
  private static final String CERT_HOLDER_ID = "cert_holder_id";
  private static final String CERTIFICATE = "certificate";
  private static final String PRIVATE_KEY = "private_key";
  private static final String DEFAULT_LEDGER_HOST = "localhost";
  private static final String DEFAULT_LEDGER_PORT = "50051";
  private static final String DEFAULT_AUDITOR_HOST = "localhost";
  private static final String DEFAULT_AUDITOR_PORT = "40051";
  private static final String DEFAULT_AUDITOR_ENABLED = "false";
  private static final String DEFAULT_CERT_HOLDER_ID = "test_holder";

  public static ClientConfig getClientConfig(Config config) {
    String configFile;
    try {
      configFile = config.getUserString(CONFIG_NAME, "config_file");
    } catch (IllegalConfigException e) {
      configFile = null;
    }
    if (configFile != null) {
      try {
        return new ClientConfig(new File(configFile));
      } catch (IOException e) {
        LOGGER.warn("failed to load the specified config file: " + configFile, e);
      }
    }

    String host = config.getUserString(CONFIG_NAME, LEDGER_HOST, DEFAULT_LEDGER_HOST);
    String port = config.getUserString(CONFIG_NAME, LEDGER_PORT, DEFAULT_LEDGER_PORT);
    String auditorEnabled =
        config.getUserString(CONFIG_NAME, AUDITOR_ENABLED, DEFAULT_AUDITOR_ENABLED);
    String auditorHost = config.getUserString(CONFIG_NAME, AUDITOR_HOST, DEFAULT_AUDITOR_HOST);
    String auditorPort = config.getUserString(CONFIG_NAME, AUDITOR_PORT, DEFAULT_AUDITOR_PORT);
    String certificate = config.getUserString(CONFIG_NAME, CERTIFICATE);
    String certHolderId = config.getUserString(CONFIG_NAME, CERT_HOLDER_ID, DEFAULT_CERT_HOLDER_ID);
    String privateKey = config.getUserString(CONFIG_NAME, PRIVATE_KEY);

    Properties properties = new Properties();
    properties.setProperty(ClientConfig.SERVER_HOST, host);
    properties.setProperty(ClientConfig.SERVER_PORT, port);
    properties.setProperty(ClientConfig.AUDITOR_ENABLED, auditorEnabled);
    properties.setProperty(ClientConfig.AUDITOR_HOST, auditorHost);
    properties.setProperty(ClientConfig.AUDITOR_PORT, auditorPort);
    properties.setProperty(ClientConfig.CERT_HOLDER_ID, certHolderId);
    properties.setProperty(ClientConfig.CERT_PATH, certificate);
    properties.setProperty(ClientConfig.PRIVATE_KEY_PATH, privateKey);

    ClientConfig clientConfig;
    try {
      clientConfig = new ClientConfig(properties);
    } catch (IOException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    return clientConfig;
  }
}
