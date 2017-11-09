package com.thedataincubator.hadoopexamples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class UnixWordCount {

  public static class WordTokenizerMapper
  extends Mapper<Object, Text, Text, IntWritable>
  {
    private final static IntWritable one = new IntWritable(1);
    private final static IntWritable integer = new IntWritable(0);
    private Text chars = new Text("chars");
    private Text words = new Text("words");
    private Text lines = new Text("lines");

    @Override
    public void map(Object key,
                    Text value,
                    Context context)
    throws IOException, InterruptedException
    {
      StringTokenizer tokenizer = new StringTokenizer(value.toString(), " \t\n\r\f,.:;?![]'");
      while (tokenizer.hasMoreTokens())
      {
        String s = tokenizer.nextToken().toLowerCase().trim();
        integer.set(s.length());
        context.write(chars, integer);
        context.write(words, one);
      }
      context.write(lines, one);
    }
  }


  public static class WordOccurrenceReducer
  extends Reducer<Text, IntWritable, Text, IntWritable>
  {
    private IntWritable occurrencesOfWord = new IntWritable();

    @Override
    public void reduce(Text key,
                       Iterable<IntWritable> values,
                       Context context)
    throws IOException, InterruptedException
    {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      occurrencesOfWord.set(sum);
      context.write(key, occurrencesOfWord);
    }
  }

  /**
   * the "driver" class. it sets everything up, then gets it started.
   */
  public static void main(String[] args)
  throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2)
    {
      System.err.println("Usage: wordcount <inputFile> <outputDir>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordTokenizerMapper.class);
    job.setCombinerClass(WordOccurrenceReducer.class);
    job.setReducerClass(WordOccurrenceReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

