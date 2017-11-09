package com.thedataincubator.hadoopexamples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/*
  This class is the union of amount and country.
  Sample values are, e.g., ("amount", 30) or ("country", "US")
*/
public class AmountOrCountry
implements Writable
{
  public AmountOrCountry(){}

  public Text type = new Text();
  public IntWritable amount = new IntWritable();
  public Text country = new Text();

  public void setAmount(int amount) {
    type.set("amount");
    this.amount = new IntWritable(amount);
  }

  public void setCountry(String country) {
    type.set("country");
    this.country = new Text(country);
  }

  public void write(DataOutput out) throws IOException {
    type.write(out);
    amount.write(out);
    country.write(out);
  }

  public void readFields(DataInput in) throws IOException {
    type.readFields(in);
    amount.readFields(in);
    country.readFields(in);
  }
}