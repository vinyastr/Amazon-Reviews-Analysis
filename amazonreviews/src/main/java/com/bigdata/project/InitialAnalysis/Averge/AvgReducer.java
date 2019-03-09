package com.bigdata.project.InitialAnalysis.Averge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgReducer extends Reducer<Text, CustomWritableAvg,Text, CustomWritableAvg> {

    CustomWritableAvg cwa = new CustomWritableAvg();


    @Override
    protected void reduce(Text key, Iterable<CustomWritableAvg> values, Context context) throws IOException, InterruptedException {
        float sum =0;
        long count=0;

        for(CustomWritableAvg v :values){
            sum+=v.getRating()* v.getCount();
            count+=v.getCount();
        }
        cwa.setRating(sum/count);
        cwa.setCount(count);

        context.write(key,cwa);
    }
}
