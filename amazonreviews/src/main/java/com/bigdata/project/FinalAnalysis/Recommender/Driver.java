package com.bigdata.project.FinalAnalysis.Recommender;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
    public static void main(String[] args) {
        try {
            //Creating a job
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setJarByClass(ProductMapper.class);


            //Assigning the mapper, reducer, combiner classes
            job.setMapperClass(ProductMapper.class);
            job.setReducerClass(ProductReducer.class);
            job.setCombinerClass(ProductReducer.class);

            //Setting Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(CustomWritableProduct.class);
            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(CustomWritableProduct.class);

            //Setting Number of Reducers
            job.setNumReduceTasks(1);

            //Specify Input Path
            FileInputFormat.addInputPath(job, new Path(args[0]));

            //Specify Output Path
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);


        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

}
