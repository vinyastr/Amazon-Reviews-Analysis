package com.bigdata.project.others.simpleRandomFilter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class RandomSampleMapper extends Mapper<Object, Text, NullWritable,Text> {

    private Random random = new Random();
    private Double percentage;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            String strPercentage = context.getConfiguration().get("filter_percentage");
            percentage = Double.parseDouble(strPercentage) / 100.0;
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try{
            String[] fields = value.toString().split("\t");


            if((random.nextDouble() < percentage) && (!fields[1].equals("marketplace")) && (!(fields[5].equals("")||fields[6].equals("")))){
                context.write(NullWritable.get(),value);
            }

        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
