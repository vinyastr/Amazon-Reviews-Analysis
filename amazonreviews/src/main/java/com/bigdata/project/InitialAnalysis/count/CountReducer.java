package com.bigdata.project.InitialAnalysis.count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
    LongWritable result = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        try{
            long sum =0;
            for(LongWritable value : values){
                sum+= value.get();
            }
            result.set(sum);
            context.write(key,result);

        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
