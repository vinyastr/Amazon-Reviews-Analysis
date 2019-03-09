package com.bigdata.project.InitialAnalysis.SDMedian;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {

    public static void main(String[] args){

        try{

            Job job = Job.getInstance();
            job.setJarByClass(RatingsMSDMapper.class);

            //Mapper Reducer
            job.setMapperClass(RatingsMSDMapper.class);
            job.setReducerClass(RatingsMSDReducer.class);
            job.setCombinerClass(RatingsMSDCombiner.class);


            //Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(SortedMapWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MedianSDWritable.class);

            //Number Of Reducers
            job.setNumReduceTasks(1);

            //Input and Out Paths

            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
