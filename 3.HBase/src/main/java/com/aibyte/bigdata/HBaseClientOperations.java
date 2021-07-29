package com.aibyte.bigdata;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
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

public class HBaseClientOperations {

  private static final String NAMESPACE = "lirui";
  private static final String TABLE_NAME_CONSTANT = "student";
  private static final TableName TABLE_NAME = TableName
      .valueOf(String.format("%s:%s", NAMESPACE, TABLE_NAME_CONSTANT));
  private static final String COLUMN_FAMILY_NAME_NAME = "name";
  private static final String COLUMN_FAMILY_NAME_INFO = "info";
  private static final String COLUMN_FAMILY_NAME_SCORE = "score";
  private static final byte[] CF_INFO_STUDENT_ID_ROW = Bytes.toBytes("student_id");
  private static final byte[] CF_INFO_CLASS_ROW = Bytes.toBytes("class");
  private static final byte[] CF_SCORE_UNDERSTANDING_ROW = Bytes.toBytes("understanding");
  private static final byte[] CF_SCORE_PROGRAMMING_ROW = Bytes.toBytes("programing");

  private static final byte[] CF_NAME_ROW_VALUE = Bytes.toBytes(NAMESPACE);
  private static final byte[] CF_INFO_STUDENT_ID_ROW_VALUE = Bytes.toBytes("20210735010084");
  private static final byte[] CF_INFO_CLASS_ROW_VALUE = Bytes.toBytes("5");
  private static final byte[] CF_SCORE_UNDERSTANDING_ROW_VALUE = Bytes.toBytes("90");
  private static final byte[] CF_SCORE_PROGRAMMING_ROW_VALUE = Bytes.toBytes("95");

  public void run(Configuration configuration) {
    try (Connection connection = ConnectionFactory.createConnection(configuration)) {
      Admin admin = connection.getAdmin();

      //1. Create namespace
      createNamespace(admin);

      // 2. Create Table here
      createTable(admin);

      Table table = connection.getTable(TABLE_NAME);
      // 3. Insert data
      put(table);
      // 4. Fetch single row
      get(table);

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void createNamespace(Admin admin) throws IOException {
    NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(NAMESPACE).build();
    // TODO: check namespace exist or not
    admin.createNamespace(namespaceDescriptor);
    System.out.println("> ===============");
    System.out.println("Create namespace success");
    System.out.println("> =============== <");
  }

  private void createTable(Admin admin) throws IOException {
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY_NAME_NAME.getBytes()).build())
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY_NAME_INFO.getBytes()).build())
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY_NAME_SCORE.getBytes()).build())
        .build();
    // delete table first
    if (admin.tableExists(TABLE_NAME) && admin.isTableDisabled(TABLE_NAME)) {
      // disable table before delete it
      admin.disableTable(TABLE_NAME);
      admin.deleteTable(TABLE_NAME);
    }

    admin.createTable(tableDescriptor);
    System.out.println("> ===============");
    System.out.println("Create table success");
    System.out.println("> =============== <");
  }

  private void put(Table table) throws IOException {
    // 3. insert data
    // https://hbase.apache.org/book.html#_implicit_version_example

    Put put = new Put(CF_NAME_ROW_VALUE);// lirui
    put.addColumn(COLUMN_FAMILY_NAME_NAME.getBytes(), null, CF_NAME_ROW_VALUE);
    put.addColumn(COLUMN_FAMILY_NAME_INFO.getBytes(), CF_INFO_STUDENT_ID_ROW,
        CF_INFO_STUDENT_ID_ROW_VALUE);
    put.addColumn(COLUMN_FAMILY_NAME_INFO.getBytes(), CF_INFO_CLASS_ROW, CF_INFO_CLASS_ROW_VALUE);
    put.addColumn(COLUMN_FAMILY_NAME_SCORE.getBytes(), CF_SCORE_UNDERSTANDING_ROW,
        CF_SCORE_UNDERSTANDING_ROW_VALUE);
    put.addColumn(COLUMN_FAMILY_NAME_SCORE.getBytes(), CF_SCORE_PROGRAMMING_ROW,
        CF_SCORE_PROGRAMMING_ROW_VALUE);
    table.put(put);
    System.out.println("> ===============");
    System.out.println("Insert data success");
    System.out.println("> =============== <");
  }

  private void get(Table table) throws IOException {
    Get get = new Get(CF_NAME_ROW_VALUE);
    Result result = table.get(get);
    System.out.println("> ===============");
    System.out.println("Fetch data success");
    System.out.println("> =============== <");
    System.out.println("===============");
    System.out.printf("-> %s", result.toString());
  }

}
