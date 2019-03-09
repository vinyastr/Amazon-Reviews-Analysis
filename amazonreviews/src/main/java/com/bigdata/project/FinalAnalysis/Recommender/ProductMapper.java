package com.bigdata.project.FinalAnalysis.Recommender;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductMapper extends Mapper<LongWritable, Text,Text,CustomWritableProduct> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String fields[] = value.toString().split("\t");
            if(!fields[0].equals("marketplace") && fields[11].equals("Y")){
                Text customerId = new Text(fields[1]);
                CustomWritableProduct cwp = new CustomWritableProduct(fields[4],fields[7]);
                context.write(customerId,cwp);
            }

        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
