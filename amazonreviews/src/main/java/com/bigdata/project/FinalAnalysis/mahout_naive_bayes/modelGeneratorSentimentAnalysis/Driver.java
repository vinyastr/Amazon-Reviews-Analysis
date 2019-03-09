package com.bigdata.project.FinalAnalysis.mahout_naive_bayes.modelGeneratorSentimentAnalysis;

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
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setJarByClass(RandomMapper.class);


            //Assigning the mapper, reducer, combiner classes
            job.setMapperClass(RandomMapper.class);
            job.setReducerClass(RandomReducer.class);

            //Setting Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //Setting Number of Reducers
            job.setNumReduceTasks(1);

            //Specify Input Path
            FileInputFormat.addInputPath(job, new Path(args[0]));

            //Specify Output Path
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
