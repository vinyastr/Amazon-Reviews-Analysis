package com.bigdata.project.others.simpleRandomFilter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class Filtering extends Mapper<Object, Text, NullWritable,Text> {

    String filter;
    int fieldNum;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
             filter = context.getConfiguration().get("filter_Value");
             fieldNum = Integer.parseInt(context.getConfiguration().get("filter_field"));
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try{
            String[] fields = value.toString().split("\t");


            if((fields[fieldNum].equalsIgnoreCase(filter)) && (!fields[0].equals("marketplace"))){
                context.write(NullWritable.get(),value);
            }

        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}

