package com.aibyte.bigdata;

import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseApplication {

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
      System.out.println("Hbase is not running");
      return;
    }

    HBaseClientOperations hBaseClientOperations = new HBaseClientOperations();
    hBaseClientOperations.run(configuration);
  }
}
