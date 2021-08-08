package com.aibyte.bigdata.hive.fileformat.geekfile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class GeekTextInputFormat implements InputFormat, JobConfigurable {

  public static final String GEEK_TEXT_INPUT_FORMAT_SIGNATURE = "geek.text.input.format.signature";
  TextInputFormat format;
  JobConf job;

  public static class GeekLineRecordReader implements RecordReader<LongWritable, BytesWritable>,
      JobConfigurable {

    LineRecordReader reader;
    Text text;
    private byte[] signature;

    public GeekLineRecordReader(LineRecordReader reader) {
      this.reader = reader;
      text = reader.createValue();
    }

    @Override
    public void configure(JobConf jobConf) {
      String signatureString = jobConf.get(GEEK_TEXT_INPUT_FORMAT_SIGNATURE);
      if (signatureString != null) {
        signature = signatureString.getBytes(StandardCharsets.UTF_8);
      } else {
        signature = new byte[0];
      }
    }

    @Override
    public boolean next(LongWritable key, BytesWritable value) throws IOException {
      while (reader.next(key, text)) {
        // text -> byte[] -> value
        byte[] textBytes = text.getBytes();
        int length = text.getLength();

        // Trim additional bytes
        if (length != textBytes.length) {
          textBytes = Arrays.copyOf(textBytes, length);
        }
        byte[] binaryData = Base64.getDecoder().decode(textBytes);

        // compare data header with signature
        int i;
        for (i = 0; i < binaryData.length && i < signature.length
            && binaryData[i] == signature[i]; ++i) {
          ;
        }

        // return the row only if it's not corrupted
        if (i == signature.length) {
          value.set(binaryData, signature.length, binaryData.length
              - signature.length);
          return true;
        }
      }
      // no more data
      return false;
    }

    @Override
    public LongWritable createKey() {
      return reader.createKey();
    }

    @Override
    public BytesWritable createValue() {
      return new BytesWritable();
    }

    @Override
    public long getPos() throws IOException {
      return reader.getPos();
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException {
      return reader.getProgress();
    }
  }

  public GeekTextInputFormat() {
    this.format = new TextInputFormat();
  }

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int i) throws IOException {
    return format.getSplits(jobConf,i);
  }

  @Override
  public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter)
      throws IOException {
    reporter.setStatus(inputSplit.toString());
    GeekLineRecordReader reader = new GeekLineRecordReader(
        new LineRecordReader(jobConf, (FileSplit) inputSplit));
    reader.configure(jobConf);
    return reader;
  }

  @Override
  public void configure(JobConf jobConf) {
    this.job = jobConf;
    format.configure(jobConf);
  }
}
