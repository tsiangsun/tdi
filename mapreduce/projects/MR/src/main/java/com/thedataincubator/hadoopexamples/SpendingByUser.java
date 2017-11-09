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
import org.apache.hadoop.util.GenericOptionsParser;


public class SpendingByUser {
  // a transaction contains these fields:
  // transactionId, productId, userId, purchaseAmount
  // example record:
  // t2 p4  u2  150
  // data is tab delimited
  public static class TransactionMapper
  extends Mapper<LongWritable, Text, Text, IntWritable>
  {
    public static Text outKey = new Text();
    public static IntWritable outValue = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
    throws java.io.IOException, InterruptedException, NumberFormatException {
      String[] record = value.toString().split("\t");
      String uid = record[2];
      int amount = Integer.parseInt(record[3]);
      outKey.set(uid);
      outValue.set(amount);
      context.write(outKey, outValue);
    }
  }

  public static class SumReducer
  extends Reducer<Text, IntWritable, Text, IntWritable>
  {
    public static Text total = new Text("total");
    public static IntWritable sumWritable = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
    throws java.io.IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value: values) {
        sum += value.get();
      }
      sumWritable.set(sum);
      context.write(total, sumWritable);
    }
  }

  public static void main(String[] args)
  throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <inputFile> <outputDir>");
      System.exit(2);
    }
    // parse input arguments for file locations
    Path input = new Path(otherArgs[1]);
    Path output = new Path(otherArgs[2]);

    // configure job
    Job job = new Job(conf, "Amount by Country");
    job.setJarByClass(BasicJoin.class);

    // set mappers and input formats
    job.setMapperClass(TransactionMapper.class);
    FileInputFormat.addInputPath(job, input);

    // give key and value classes between map and reduce
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    // set reducers, output format, output key and value pairs, and output path
    job.setReducerClass(SumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, output);

    if (job.waitForCompletion(true)) return;
    else throw new Exception("First Job Failed");
  }
}