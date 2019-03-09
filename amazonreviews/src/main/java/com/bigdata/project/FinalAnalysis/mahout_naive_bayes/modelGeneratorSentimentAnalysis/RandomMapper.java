package com.bigdata.project.FinalAnalysis.mahout_naive_bayes.modelGeneratorSentimentAnalysis;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RandomMapper extends Mapper<LongWritable, Text, Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text t = new Text();
        String[] fields = value.toString().split("\t");
        if((fields[7].equals("1") || fields[7].equals("5")) && !fields[13].equals("")) {
            if(fields[7].equals("1")){
                t.set("negative");
            }
            else {
                t.set("positive");
            }
            System.out.println(t);
            Text r = new Text(fields[13]);
            context.write(t, r);
        }
    }
}
