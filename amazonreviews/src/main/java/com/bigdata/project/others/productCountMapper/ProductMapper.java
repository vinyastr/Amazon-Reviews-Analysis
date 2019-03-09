package com.bigdata.project.others.productCountMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductMapper extends Mapper<LongWritable, Text,LongWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        Text t = new Text(fields[0]);
        LongWritable l = new LongWritable(Long.parseLong(fields[1]));

        context.write(l,t);

    }
}
