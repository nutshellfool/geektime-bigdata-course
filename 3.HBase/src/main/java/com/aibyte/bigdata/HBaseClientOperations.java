package com.aibyte.bigdata;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final byte[] CF_NAME_ROW_VALUE_TOM = Bytes.toBytes("Tom");
  private static final byte[] CF_INFO_STUDENT_ID_ROW_VALUE_TOM = Bytes.toBytes("20210000000001");
  private static final byte[] CF_INFO_CLASS_ROW_VALUE_TOM = Bytes.toBytes("1");
  private static final byte[] CF_SCORE_UNDERSTANDING_ROW_VALUE_TOM = Bytes.toBytes("75");
  private static final byte[] CF_SCORE_PROGRAMMING_ROW_VALUE_TOM = Bytes.toBytes("82");

  private static final Logger logger
      = LoggerFactory.getLogger(HBaseClientOperations.class);

  public void run(Configuration configuration) {
    logger.debug("run ------>");
    try (Connection connection = ConnectionFactory.createConnection(configuration)) {
      Admin admin = connection.getAdmin();

      //0. Clean up existed deployment
      cleanupEnv(admin);

      //1. Create namespace
      createNamespace(admin);

      // 2. Create Table here
      createTable(admin);

      Table table = connection.getTable(TABLE_NAME);

      // 3. Insert data
      put(table);
      putCellVersion(table);

      // 4. Fetch single row
      get(table);

      // 5. Delete row
      deleteSingleRow(table);

    } catch (IOException e) {
      e.printStackTrace();
    }
    logger.debug("-----> end");
  }

  private void cleanupEnv(Admin admin) throws IOException {
    logger.debug("> Clean up env ===============");
    deleteTable(admin);
    admin.deleteNamespace(NAMESPACE);
    logger.debug(" Create up env  success");
  }

  private void createNamespace(Admin admin) throws IOException {
    logger.debug("> Create namespace ===============");

    NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(NAMESPACE).build();
    admin.createNamespace(namespaceDescriptor);

    logger.debug(" Create namespace success");
  }

  private void createTable(Admin admin) throws IOException {
    logger.debug("> Create table ===============");

    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TABLE_NAME)
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY_NAME_NAME.getBytes()).build())
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY_NAME_INFO.getBytes()).build())
        .setColumnFamily(
            ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY_NAME_SCORE.getBytes()).build())
        .build();

    admin.createTable(tableDescriptor);

    logger.debug(" Create table success");
  }

  private void deleteTable(Admin admin) throws IOException {
    logger.debug("> Delete table ===============");
    // delete table first
    if (admin.tableExists(TABLE_NAME) && !admin.isTableDisabled(TABLE_NAME)) {
      // disable table before delete it
      admin.disableTable(TABLE_NAME);
      admin.deleteTable(TABLE_NAME);
    }
    logger.debug("Delete table success");
  }

  private void put(Table table) throws IOException {
    // https://hbase.apache.org/book.html#_implicit_version_example
    logger.debug("> Insert data ===============");

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

    logger.debug(" Insert success");
  }

  private void putCellVersion(Table table) throws IOException {
    logger.debug("> Put data cell version ===============");

    Put put = new Put(CF_NAME_ROW_VALUE_TOM);
    put.add(
        CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setType(Type.Put)
            .setRow(CF_NAME_ROW_VALUE_TOM)
            .setFamily(COLUMN_FAMILY_NAME_NAME.getBytes())
            .setValue(CF_NAME_ROW_VALUE_TOM)
            .build());
    put.add(
        CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setType(Type.Put)
            .setRow(CF_NAME_ROW_VALUE_TOM)
            .setQualifier(CF_INFO_STUDENT_ID_ROW)
            .setFamily(COLUMN_FAMILY_NAME_INFO.getBytes())
            .setValue(CF_INFO_STUDENT_ID_ROW_VALUE_TOM)
            .build());
    put.add(
        CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setType(Type.Put)
            .setRow(CF_NAME_ROW_VALUE_TOM)
            .setQualifier(CF_INFO_CLASS_ROW)
            .setFamily(COLUMN_FAMILY_NAME_INFO.getBytes())
            .setValue(CF_INFO_CLASS_ROW_VALUE_TOM)
            .build());
    put.add(
        CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setType(Type.Put)
            .setRow(CF_NAME_ROW_VALUE_TOM)
            .setQualifier(CF_SCORE_PROGRAMMING_ROW)
            .setFamily(COLUMN_FAMILY_NAME_SCORE.getBytes())
            .setValue(CF_SCORE_PROGRAMMING_ROW_VALUE_TOM)
            .build());
    put.add(
        CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setType(Type.Put)
            .setRow(CF_NAME_ROW_VALUE_TOM)
            .setQualifier(CF_SCORE_UNDERSTANDING_ROW)
            .setFamily(COLUMN_FAMILY_NAME_SCORE.getBytes())
            .setValue(CF_SCORE_UNDERSTANDING_ROW_VALUE_TOM)
            .build());
    table.put(put);
    logger.debug("Put data cell version success");
  }

  private void get(Table table) throws IOException {
    logger.debug("> Fetch data ===============");

    Get get = new Get(CF_NAME_ROW_VALUE);
    Result result = table.get(get);

    logger.debug(" Fetch data success");
    logger.debug("===============");
    logger.debug("-> {}", result);
  }

  private void deleteSingleRow(Table table) throws IOException {
    logger.debug("> Delete data ===============");

    Delete delete = new Delete(CF_NAME_ROW_VALUE_TOM);
    delete.addColumn(COLUMN_FAMILY_NAME_NAME.getBytes(), null);
    delete.addColumn(COLUMN_FAMILY_NAME_INFO.getBytes(), CF_INFO_STUDENT_ID_ROW);
    delete.addColumn(COLUMN_FAMILY_NAME_INFO.getBytes(), CF_INFO_CLASS_ROW);
    delete.addColumn(COLUMN_FAMILY_NAME_SCORE.getBytes(), CF_SCORE_UNDERSTANDING_ROW);
    delete.addColumn(COLUMN_FAMILY_NAME_SCORE.getBytes(), CF_SCORE_PROGRAMMING_ROW);
    table.delete(delete);

    logger.debug(" Delete data success");
  }

}
