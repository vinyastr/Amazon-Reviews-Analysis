package com.bigdata.project.FinalAnalysis.Recommender;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class ProductReducer extends Reducer<Text,CustomWritableProduct,Text,CustomWritableProduct> {

    @Override
    protected void reduce(Text key, Iterable<CustomWritableProduct> values, Context context) throws IOException, InterruptedException {
        try {
            for (CustomWritableProduct cwp : values) {
                context.write(key, cwp);
                }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
