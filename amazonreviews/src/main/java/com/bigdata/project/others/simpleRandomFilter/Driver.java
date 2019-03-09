package com.bigdata.project.others.simpleRandomFilter;


import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;


public class Driver {
    public static void main(String[] args) {
        try {
            //Creating a job
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);

            //local File System instance
            FileSystem local = FileSystem.getLocal(conf);

            //Path to the directory in local file System
            Path inputDir = new Path(args[0]);
            Path hdfsFile = new Path(args[1]);

            try {
                //List of Files Names from FileStatus
                FileStatus[] inputFiles = local.listStatus(inputDir);
                //Writing to hdfs using OutputStream
                FSDataOutputStream out = hdfs.create(hdfsFile);

                //Reading the input files using FSDataInputStream
                for (int i = 0; i < inputFiles.length; i++) {
                    FSDataInputStream in = local.open(inputFiles[i].getPath());
                    byte buffer[] = new byte[256];
                    int bytesRead = 0;

                    while ((bytesRead = in.read(buffer)) > 0) {

                        out.write(buffer, 0, bytesRead);
                    }
                    in.close();
                }
                out.close();

            }catch(Exception e){
                e.printStackTrace();
            }

            conf.set("filter_percentage","10");
            conf.set("Count","1000");
            Job job = Job.getInstance(conf);
            job.setJarByClass(RandomSampleMapper.class);


            //Assigning the mapper, reducer, combiner classes
            job.setMapperClass(RandomSampleMapper.class);
            job.setReducerClass(RandomSampleReducer.class);

            //Setting Input and Output formats
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            //Setting Number of Reducers
            job.setNumReduceTasks(1);

            //Specify Input Path
            FileInputFormat.addInputPath(job, new Path(args[1]));

            //Specify Output Path
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

}
