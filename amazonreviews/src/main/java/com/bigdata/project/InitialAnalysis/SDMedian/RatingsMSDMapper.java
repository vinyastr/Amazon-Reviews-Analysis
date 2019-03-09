package com.bigdata.project.InitialAnalysis.SDMedian;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RatingsMSDMapper extends Mapper<LongWritable,Text,Text, SortedMapWritable> {

    private IntWritable rating = new IntWritable();
    private static final  LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        if(!fields[0].equals("marketplace")&& !fields[7].equals("star_rating")) {
            Text productId = new Text(fields[6]);
            rating.set(Integer.parseInt(fields[7]));

            SortedMapWritable outSorted = new SortedMapWritable();
            outSorted.put(rating, one);

            context.write(productId, outSorted);
        }

    }
}
