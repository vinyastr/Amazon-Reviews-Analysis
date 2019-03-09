package com.bigdata.project.FinalAnalysis.yearlyWiseTrend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Top5ProductsByYear {

    public static class CustomWritableCountClass implements WritableComparable<CustomWritableCountClass> {
        private String productId;
        private Integer year;
        private Long count;

        public CustomWritableCountClass() {
        }

        public CustomWritableCountClass(String productId, Integer year, Long count) {
            this.productId = productId;
            this.year = year;
            this.count = count;
        }

        public String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        public Integer getYear() {
            return year;
        }

        public void setYear(Integer year) {
            this.year = year;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        public int compareTo(CustomWritableCountClass o) {
            int result =0;
//            result = year.compareTo(o.year);
//            if(result==0){
                result = -1 *count.compareTo(o.count);
//            }
            return result;}

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(productId);
            dataOutput.writeInt(year);
            dataOutput.writeLong(count);

        }

        public void readFields(DataInput dataInput) throws IOException {
            productId = dataInput.readUTF();
            year = dataInput.readInt();
            count = dataInput.readLong();

        }

        @Override
        public String toString() {
            return productId +"\t"+ year+"\t" + count;
        }
    }


    public static class Top5ByYearMapper extends Mapper<LongWritable, Text,CustomWritableCountClass, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           try {
               String[] fields = value.toString().split("\t");
                String prodId = fields[0];
                int year = Integer.parseInt(fields[1]);
                long count = Long.parseLong(fields[2]);

                CustomWritableCountClass ccw = new CustomWritableCountClass(prodId,year,count);
                context.write(ccw,new IntWritable(1));
           }catch(Exception ex){
               ex.printStackTrace();
           }
        }
    }

    public static class Top5ByYearReducer extends Reducer<CustomWritableCountClass,IntWritable,CustomWritableCountClass,IntWritable> {
        public int count =1;
        @Override
        protected void reduce(CustomWritableCountClass key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for(IntWritable v:values){
                if(count<26){
                 context.write(key,new IntWritable(count));
                 count++;
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf3 = new Configuration();
            Job job2 = Job.getInstance(conf3, "Top5");


            job2.setJarByClass(Top5ProductsByYear.class);


            //Assigning the mapper, reducer, combiner classes
            job2.setMapperClass(Top5ByYearMapper.class);
            job2.setReducerClass(Top5ByYearReducer.class);
            job2.setCombinerClass(Top5ByYearReducer.class);

            //Setting Input and Output formats
            job2.setInputFormatClass(TextInputFormat.class);
            job2.setOutputFormatClass(TextOutputFormat.class);
            job2.setOutputKeyClass(CustomWritableCountClass.class);
            job2.setOutputValueClass(IntWritable.class);
            job2.setMapOutputKeyClass(CustomWritableCountClass.class);
            job2.setMapOutputValueClass(IntWritable.class);


            //Setting Number of Reducers
            job2.setNumReduceTasks(1);

            //Specify Input Path
            FileInputFormat.addInputPath(job2, new Path(args[0]));

            //Specify Output Path
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);

        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}
