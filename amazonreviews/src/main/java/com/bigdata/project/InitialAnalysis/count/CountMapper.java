package com.bigdata.project.InitialAnalysis.count;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    String filter;
    int fieldNum;
    boolean filterCond;
    String fieldName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            String filCond = context.getConfiguration().get("filter_cond");
            if(filCond.equals("N")){
                filterCond=false;
            }
            else{
                filterCond=true;
            }
            fieldName = context.getConfiguration().get("field_Name");
            filter = context.getConfiguration().get("filter_Value");
            fieldNum = Integer.parseInt(context.getConfiguration().get("filter_field"));
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        try {

            String field = fields[fieldNum];
            if((!field.equals(fieldName))||(!field.equals(""))) {
                if(!filterCond) {
                    Text c = new Text(field);
                    LongWritable count = new LongWritable(1);

                    context.write(c, count);
                }
                else {
                    if(fields[fieldNum].equals(filter)){
                        Text c = new Text(field);
                        LongWritable count = new LongWritable(1);

                        context.write(c, count);
                    }
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
