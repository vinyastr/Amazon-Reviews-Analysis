package com.bigdata.project.FinalAnalysis.mahout_naive_bayes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SentimentalAnalysis {

	public static void main(String[] args) throws Exception {

		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Review Sentiment Analysis");
			job.setJarByClass(SentimentalAnalysis.class);
			job.setMapperClass(SentimentAnalysisMapper.class);
			job.setReducerClass(SentimentAnalysisReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			// cleanUpOutputDiectory(conf, args[1]);
			System.exit(job.waitForCompletion(false) ? 0 : 1);
		} catch (InterruptedException e) {
			System.out.println(e);
		} catch (ClassNotFoundException e) {
			System.out.println(e);
		}
	}

	public static class SentimentAnalysisMapper extends Mapper<Object, Text, Text,IntWritable> {

		NaiveBayes nv;

		@Override
		protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			this.nv = new NaiveBayes("/Users/vinyaskaushiktr/Downloads/model/part-r-00000");
			try {
				nv.trainModel();
			} catch (Exception e) {
				System.out.println(e);
			}
		}

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");

			Text keyOut = new Text();
			IntWritable valueOut = new IntWritable();
			try {
				keyOut.set((split[3]));
//				System.out.println(keyOut);
				valueOut.set(nv.classifyNewReview(split[13]));
//				System.out.println(++count);
			} catch (Exception e) {
				e.printStackTrace();
			}
			context.write(keyOut, valueOut);
		}
	}

	public static class SentimentAnalysisReducer
			extends Reducer<Text, IntWritable, Text, DoubleWritable> {

		@Override
		protected void reduce(Text keyIn, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int numberOfReviews = 0;
			int countOfPositiveReviews = 0;
			for (IntWritable val : values) {
				countOfPositiveReviews += val.get();
				numberOfReviews = numberOfReviews + 1;
			}
			float percentageOfPositiveReviews = ((float) countOfPositiveReviews / numberOfReviews) * 100;
			System.out.println(keyIn +" "+percentageOfPositiveReviews);
			context.write(keyIn, new DoubleWritable(percentageOfPositiveReviews));
		}

	}

}
