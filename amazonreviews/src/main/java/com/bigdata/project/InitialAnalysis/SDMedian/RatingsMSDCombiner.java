package com.bigdata.project.InitialAnalysis.SDMedian;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;

public class RatingsMSDCombiner extends Reducer<Text, SortedMapWritable,Text,SortedMapWritable> {

    @Override
    protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
        SortedMapWritable outValue = new SortedMapWritable();

        for(SortedMapWritable v:values){
            for(Object e :v.entrySet()){
            Map.Entry<WritableComparable,Writable> entry =  (Map.Entry<WritableComparable,Writable>)e;
                LongWritable count = (LongWritable) outValue.get(entry.getKey());

                if(count!=null){
                    count.set(count.get()+((LongWritable)entry.getValue()).get());
                }
                else {
                    outValue.put(entry.getKey(),new LongWritable(((LongWritable)entry.getValue()).get()));
                }
            }
            v.clear();
        }
        context.write(key,outValue);
    }
}
