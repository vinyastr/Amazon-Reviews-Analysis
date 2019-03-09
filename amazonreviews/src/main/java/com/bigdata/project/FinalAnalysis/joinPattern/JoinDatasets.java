package com.bigdata.project.FinalAnalysis.joinPattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinDatasets {

    public static class ProductMapper extends Mapper<LongWritable,Text,Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String productId = fields[1];
            Text product = new Text(productId);
            Text t = new Text("A");
            context.write(product,t);
        }
    }

    public static class DataMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String productId = fields[3];
            Text product = new Text(productId);
            context.write(product,value);
        }
    }

    public static class JoinReducer extends Reducer<Text,Text,Text,Text>{

        List<Text> listA = new ArrayList<Text>();
        List<Text>listB = new ArrayList<Text>();
        Text EMPTYTEXT = new Text("");

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for(Text value:values){
                if(value.toString().equals("A")){
                    listA.add(value);
                }
                else {
                    listB.add(value);
                }
            }
         if(!listA.isEmpty() && !listB.isEmpty())   {
            for(Text a:listA){
                for(Text b:listB) {
                    context.write(b, a);
                }
                }
            }
        }
    }

    public static void main(String[] args){
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setJarByClass(JoinDatasets.class);


            //Assigning the mapper, reducer, combiner classes
            job.setReducerClass(JoinReducer.class);
            MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,ProductMapper.class);
            MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,DataMapper.class);

            //Setting Input and Output formats
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //Setting Number of Reducers
//            job.setNumReduceTasks(1);

            //Specify Output Path
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
