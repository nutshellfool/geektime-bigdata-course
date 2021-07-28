package com.aibyte.bigdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseApplication {

  public static void main(String[] args) {

    // Constants
    String namespace = "lirui";
    String tableNameConstant = "student";
    TableName tableName = TableName.valueOf(String.format("%s:%s", namespace, tableNameConstant));
    String columnFamilyNameName = "name";
    String columnFamilyNameInfo = "info";
    String columnFamilyNameScore = "score";
    byte[] cfNameRow = Bytes.toBytes("name");
    byte[] cfInfoStudentIDRow = Bytes.toBytes("student_id");
    byte[] cfInfoClassRow = Bytes.toBytes("class");
    byte[] cfScoreUnderstandingRow = Bytes.toBytes("understanding");
    byte[] cfScoreProgrammingRow = Bytes.toBytes("programing");

    byte[] cfNameRowValue = Bytes.toBytes("lirui");
    byte[] cfInfoStudentIDRowValue = Bytes.toBytes("20210735010084");
    byte[] cfInfoClassRowValue = Bytes.toBytes("5");
    byte[] cfScoreUnderstandingRowValue = Bytes.toBytes("90");
    byte[] cfScoreProgrammingRowValue = Bytes.toBytes("95");

    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", Constants.HBASE_ZOOKEEPER_QUORUM);
    configuration
        .set("hbase.zookeeper.property.clientPort", Constants.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT);

    try (Connection connection = ConnectionFactory.createConnection(configuration)) {
      Admin admin = connection.getAdmin();

      //1. Create namespace
      NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
      // TODO: check namespace exist or not
      admin.createNamespace(namespaceDescriptor);
      System.out.println("===============");
      System.out.println("Create namespace success");
      System.out.println("===============");

      // 2. Create Table here
      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
          .setColumnFamily(
              ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyNameName.getBytes()).build())
          .setColumnFamily(
              ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyNameInfo.getBytes()).build())
          .setColumnFamily(
              ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyNameScore.getBytes()).build())
          .build();
      // delete table first
      if (admin.tableExists(tableName)) {
        if (admin.isTableDisabled(tableName)) {
          // disable table before delete it
          admin.disableTable(tableName);
          admin.deleteTable(tableName);
        }
      }

      admin.createTable(tableDescriptor);
      System.out.println("===============");
      System.out.println("Create table success");
      System.out.println("===============");

      // insert data
      // https://hbase.apache.org/book.html#_implicit_version_example
      Table table = connection.getTable(tableName);
      Put put = new Put(cfNameRowValue);// lirui
      put.addColumn(columnFamilyNameName.getBytes(), null, cfNameRowValue);
      put.addColumn(columnFamilyNameInfo.getBytes(), cfInfoStudentIDRow, cfInfoStudentIDRowValue);
      put.addColumn(columnFamilyNameInfo.getBytes(), cfInfoClassRow, cfInfoClassRowValue);
      put.addColumn(columnFamilyNameScore.getBytes(), cfScoreUnderstandingRow,
          cfScoreUnderstandingRowValue);
      put.addColumn(columnFamilyNameScore.getBytes(), cfScoreProgrammingRow,
          cfScoreProgrammingRowValue);
      table.put(put);
      System.out.println("===============");
      System.out.println("Insert data success");
      System.out.println("===============");

      // Fetch single row
      Get get = new Get(cfNameRowValue);
      Result result = table.get(get);
      System.out.println("===============");
      System.out.println("Fetch single row success");
      System.out.println("===============");
      System.out.println("===============");
      System.out.printf("%s : GET result", cfNameRowValue);
      System.out.printf("-> %s", result.toString());

    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
