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


public class BasicJoin {

  // structure of a user record is this:
  // id, email, language, location
  // example record:
  // u1 bob@exmaple.com EN  US
  // data is tab delimited
  public static class CountryMapper
  extends Mapper<LongWritable, Text, Text, AmountOrCountry>
  {
    public static Text outKey = new Text();
    public static AmountOrCountry outValue = new AmountOrCountry();

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws java.io.IOException, InterruptedException {
      String[] record = value.toString().split("\t");
      String uid = record[0];
      String country = record[3];
      outKey.set(uid);
      outValue.setCountry(country);
      context.write(outKey, outValue);
    }
  }

  // a transaction contains these fields:
  // transactionId, productId, userId, purchaseAmount
  // example record:
  // t2 p4  u2  150
  // data is tab delimited
  public static class AmountMapper
  extends Mapper<LongWritable, Text, Text, AmountOrCountry>
  {
    public static Text outKey = new Text();
    public static AmountOrCountry outValue = new AmountOrCountry();

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws java.io.IOException, InterruptedException, NumberFormatException {
      String[] record = value.toString().split("\t");
      String uid = record[2];
      int amount = Integer.parseInt(record[3]);
      outKey.set(uid);
      outValue.setAmount(amount);
      context.write(outKey, outValue);
    }
  }

  public static class JoinReducer
  extends Reducer<Text, AmountOrCountry, Text, IntWritable>
  {
    public static Text country = new Text("");
    public static IntWritable sumWritable = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<AmountOrCountry> values, Context context)
    throws java.io.IOException, InterruptedException {
      int sum = 0;
      for (AmountOrCountry value: values) {
        if (value.type.toString().equals("amount")) {
          sum += value.amount.get();
        } else { // "country"
          country.set(value.country.toString());
        }
      }
      sumWritable.set(sum);
      context.write(country, sumWritable);
    }
  }

  public static void main(String[] args)
  throws Exception
  {
    // parse input arguments for file locations
    Path users = new Path(args[0]);
    Path transactions = new Path(args[1]);
    Path output = new Path(args[2]);

    // configure job
    Configuration conf = new Configuration();
    Job job = new Job(conf);
    job.setJarByClass(BasicJoin.class);
    job.setJobName("Amount by Country");

    // set mappers and input formats
    MultipleInputs.addInputPath(job, transactions, TextInputFormat.class, AmountMapper.class);
    MultipleInputs.addInputPath(job, users, TextInputFormat.class, CountryMapper.class);

    // give key and value classes between map and reduce
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AmountOrCountry.class);

    // set reducers, output format, output key and value pairs, and output path
    job.setReducerClass(JoinReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, output);

    if (job.waitForCompletion(true)) return;
    else throw new Exception("First Job Failed");
  }
}