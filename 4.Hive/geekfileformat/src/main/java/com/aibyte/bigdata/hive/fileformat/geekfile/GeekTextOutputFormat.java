package com.aibyte.bigdata.hive.fileformat.geekfile;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.util.Progressable;

public class GeekTextOutputFormat<K extends WritableComparable, V extends Writable>
    extends HiveIgnoreKeyTextOutputFormat<K, V> {

  public static class GeekRecordWriter implements RecordWriter, JobConfigurable {

    public static final String GEEK_TEXT_OUTPUT_FORMAT_SIGNATURE = "geek.text.output.format.signature";
    RecordWriter writer;
    BytesWritable bytesWritable;
    private byte[] signature;

    public GeekRecordWriter(RecordWriter writer) {
      this.writer = writer;
      this.bytesWritable = new BytesWritable();
    }

    @Override
    public void initialize(OutputStream outputStream, Configuration configuration)
        throws IOException {

    }

    @Override
    public void write(Writable w) throws IOException {

      // Get input data
      byte[] input;
      int inputLength;
      if (w instanceof Text) {
        input = ((Text) w).getBytes();
        inputLength = ((Text) w).getLength();
      } else {
        assert (w instanceof BytesWritable);
        input = ((BytesWritable) w).getBytes();
        inputLength = ((BytesWritable) w).getLength();
      }

      // Add signature
      byte[] wrapped = new byte[signature.length + inputLength];
      for (int i = 0; i < signature.length; i++) {
        wrapped[i] = signature[i];
      }
      for (int i = 0; i < inputLength; i++) {
        wrapped[i + signature.length] = input[i];
      }

      // Encode
      byte[] output = Base64.getEncoder().encode(wrapped);
      bytesWritable.set(output, 0, output.length);

      writer.write(bytesWritable);
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }

    @Override
    public void configure(JobConf jobConf) {
      String signatureString = jobConf.get(GEEK_TEXT_OUTPUT_FORMAT_SIGNATURE);
      if (signatureString != null) {
        signature = signatureString.getBytes(StandardCharsets.UTF_8);
      } else {
        signature = new byte[0];
      }
    }
  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {

    GeekRecordWriter writer = new GeekRecordWriter((RecordWriter) super
        .getHiveRecordWriter(jc, finalOutPath, BytesWritable.class,
            isCompressed, tableProperties, progress));
    writer.configure(jc);
    return (FileSinkOperator.RecordWriter)writer;
  }

}
