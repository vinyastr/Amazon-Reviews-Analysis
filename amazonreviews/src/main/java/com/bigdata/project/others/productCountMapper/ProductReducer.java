package com.bigdata.project.others.productCountMapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ProductReducer extends Reducer<LongWritable, Text, LongWritable,Text> {

//    LongWritable l = new LongWritable();
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if(key.get()>10 && key.get()<100) {
            for (Text lw : values) {
                context.write(key, lw);
            }
        }
    }
}
