package com.bigdata.project.others.simpleRandomFilter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RandomSampleReducer extends Reducer<NullWritable, Text,NullWritable,Text> {
    int count=0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            String strCount = context.getConfiguration().get("Count");
            count = Integer.parseInt(strCount);
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
       int localCount=0;
        for(Text value:values){
            if(localCount<count) {
                context.write(key, value);
                localCount++;
            }
            else break;
        }
    }
}
