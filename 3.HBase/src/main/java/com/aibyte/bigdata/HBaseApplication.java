package com.aibyte.bigdata;

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseApplication {

  private static final Logger logger
      = LoggerFactory.getLogger(HBaseApplication.class);

  public static void main(String[] args) {
    new HBaseApplication().connect();
  }

  private void connect() {
    Configuration configuration = HBaseConfiguration.create();
    String path = Objects
        .requireNonNull(this.getClass().getClassLoader()
            .getResource("hbase-site.xml"))
        .getPath();

    configuration.addResource(new Path(path));
    try {
      HBaseAdmin.available(configuration);
    } catch (IOException e) {
      e.printStackTrace();
      logger.error("Hbase is not running");
      return;
    }

    HBaseClientOperations hBaseClientOperations = new HBaseClientOperations();
    hBaseClientOperations.run(configuration);
  }
}
