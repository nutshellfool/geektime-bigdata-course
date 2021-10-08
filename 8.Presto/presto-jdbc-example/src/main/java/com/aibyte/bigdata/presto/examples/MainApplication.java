package com.aibyte.bigdata.presto.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MainApplication {

  public static void main(String[] args) {

    // -----------------
    // With aliyun EMR help document
    // https://help.aliyun.com/document_detail/108859.html
    // -----------------

    try {
      Class.forName("com.facebook.presto.jdbc.PrestoDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(-1);
    }

//    String url = "jdbc:presto://106.15.194.185:9090/hive/default";
    String url = "jdbc:presto://emr-header-1:9090/hive/default";
    String sql = "SELECT cardinality(merge(cast(hll AS HyperLogLog))) AS weekly_unique_users\n"
        + "FROM visit_summaries\n"
        + "WHERE visit_date >= current_date - interval '7' day";
    try (Connection connection = DriverManager.getConnection(url, "hadoop", null)) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          int columnNum = resultSet.getMetaData().getColumnCount();
          int rowIndex = 0;
          while (resultSet.next()) {
            rowIndex++;
            for (int i = 1; i <= columnNum; i++) {
              System.out.println("Row " + rowIndex + ", Column " + i + ": " + resultSet.getInt(i));
            }
          }
        }
      }
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
  }

}
