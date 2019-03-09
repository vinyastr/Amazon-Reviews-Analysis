package com.bigdata.project.others.productCountMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {

    public static class ProductCountMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String productId = fields[3];
            Text t = new Text(productId);
            context.write(t,new LongWritable(1));
        }
    }

    public static class ProductCountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
        LongWritable lw = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum=0;
            for(LongWritable l :values){
                sum += l.get();
            }
            lw.set(sum);
            context.write(key,lw);

        }
    }



    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf,"InitialFil Creation");
            job.setJarByClass(Driver.class);

            job.setMapperClass(ProductCountMapper.class);
            job.setReducerClass(ProductCountReducer.class);
            job.setCombinerClass(ProductCountReducer.class);
//            job1.setGroupingComparatorClass(GroupSort.class);

            //Setting Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);

            //Setting Number of Reducers
//            job1.setNumReduceTasks(1);

            //Specify Input Path
            FileInputFormat.addInputPath(job, new Path(args[0]));

            //Specify Output Path
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            int result = job.waitForCompletion(true) ? 0 :1;

            if(result==0) {

                Job job1 = Job.getInstance(conf, "FinalFile Creation");
                job1.setJarByClass(Driver.class);

                job1.setMapperClass(ProductMapper.class);
                job1.setReducerClass(ProductReducer.class);
                job1.setCombinerClass(ProductReducer.class);
//            job1.setGroupingComparatorClass(GroupSort.class);

                //Setting Input and Output formats
                job1.setInputFormatClass(TextInputFormat.class);
                job1.setOutputFormatClass(TextOutputFormat.class);
                job1.setOutputKeyClass(LongWritable.class);
                job1.setOutputValueClass(Text.class);
                job1.setMapOutputKeyClass(LongWritable.class);
                job1.setMapOutputValueClass(Text.class);

                //Setting Number of Reducers
//            job1.setNumReduceTasks(1);

                //Specify Input Path
                FileInputFormat.addInputPath(job1, new Path(args[1]));

                //Specify Output Path
                FileOutputFormat.setOutputPath(job1, new Path(args[2]));

                System.exit(job1.waitForCompletion(true) ? 0 : 1);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
