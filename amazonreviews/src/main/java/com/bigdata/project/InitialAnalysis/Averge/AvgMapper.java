package com.bigdata.project.InitialAnalysis.Averge;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgMapper extends Mapper<LongWritable, Text,Text, CustomWritableAvg> {

    String filter;
    int avgFieldNum;
    int keyFieldNum;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            filter = context.getConfiguration().get("filter_Value");
            avgFieldNum = Integer.parseInt(context.getConfiguration().get("avg_field"));
            keyFieldNum = Integer.parseInt(context.getConfiguration().get("key_field"));
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String[] fields = value.toString().split("\t");
            if ((!fields[keyFieldNum].equals(filter) && (!fields[avgFieldNum].equals("")))) {
                Text keyField = new Text(fields[keyFieldNum]);
                CustomWritableAvg cwa = new CustomWritableAvg(Long.parseLong(fields[avgFieldNum]),1);

                context.write(keyField,cwa);
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
