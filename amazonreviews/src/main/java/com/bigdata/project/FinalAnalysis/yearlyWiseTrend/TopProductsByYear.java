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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopProductsByYear {

    public static class CustomWritableKeyClass implements Writable, WritableComparable<CustomWritableKeyClass> {

        private String productId;
        private Integer year;

        public CustomWritableKeyClass() {
        }

        public CustomWritableKeyClass(String productId, int year) {
            this.productId = productId;
            this.year = year;
        }

        public String getProductId() {
            return productId;
        }

        public void setProductId(String productId) {
            this.productId = productId;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }

        public int compareTo(CustomWritableKeyClass o) {
            int result = 0;
            result = year.compareTo(o.year);
            if (result == 0) {
                result = productId.compareTo(o.productId);
            }
            return result;
        }

        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(productId);
            dataOutput.writeInt(year);

        }

        public void readFields(DataInput dataInput) throws IOException {
            productId = dataInput.readUTF();
            year = dataInput.readInt();
        }

        @Override
        public String toString() {
            return productId + "\t" + year;
        }
    }

    public static class ProductReviewsYearlyMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                String[] fields = value.toString().split("\t");
                StringBuilder sb = new StringBuilder();
                if ((Integer.parseInt(fields[8]) > 3 && Integer.parseInt(fields[7]) > 3) && fields[11].equals("Y")) {
                    String prodid = fields[3];
                    for (int i = 0; i < fields.length; i++) {
                        if (i != 3 && i != 9 && i != 10 && i != 4 && i != 5) {
                            sb.append(fields[i] + "\t");

                        }
                    }

                    Text productId = new Text(prodid);
                    Text values = new Text(sb.toString());

                    context.write(productId, values);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static class ReviewCountOnProductIdMapper extends Mapper<LongWritable, Text, CustomWritableKeyClass, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                String[] fields = value.toString().split("\t");
                String prodId = fields[0];
                String[] datefields = fields[10].split("-");
                int year = Integer.parseInt(datefields[0]);

                CustomWritableKeyClass cwk = new CustomWritableKeyClass(prodId, year);

                LongWritable count = new LongWritable(1);
                context.write(cwk, count);


            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public static class ReviewCountOnProductIdReducer extends Reducer<CustomWritableKeyClass, LongWritable, CustomWritableKeyClass, LongWritable> {
        LongWritable count = new LongWritable();

        //        CustomWritableKeyClass cwk = new CustomWritableKeyClass();
//        static int top5counter=1;
        @Override
        protected void reduce(CustomWritableKeyClass key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            try {

                long sum = 0;
                for (LongWritable c : values) {
                    sum += c.get();
                }

                count.set(sum);
                context.write(key, count);


            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }



//    public static class CustomPartitioner extends Partitioner<CustomWritableCountClass, LongWritable> {
//        public int getPartition(CustomWritableCountClass arg0, LongWritable arg1, int numOfReducerTasks) {
//            // TODO Auto-generated method stub
//            int hash = arg0.year.hashCode();
//            int partition = hash % numOfReducerTasks;
//            return partition;
//        }
//    }

    public static class BinningByYear extends Mapper<Object,Text,Text,NullWritable>{
        private MultipleOutputs<Text,NullWritable> mos = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<Text, NullWritable>(context);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
                int year = Integer.parseInt(fields[1]);

                if(year == 2012){
                    mos.write("bins",value,NullWritable.get(),"2012data");
                }
                if(year ==2013){
                    mos.write("bins",value,NullWritable.get(),"2013data");
                }
                if(year == 2014){
                    mos.write("bins",value,NullWritable.get(),"2014data");
                }
                if(year==2015){
                    mos.write("bins",value,NullWritable.get(),"2015data");
                }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }


        public static void main(String[] args) {
            try {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf,"Filtering");
                job.setJarByClass(TopProductsByYear.class);


                //Assigning the mapper, reducer, combiner classes
                job.setMapperClass(ProductReviewsYearlyMapper.class);
//            job.setReducerClass(AvgReducer.class);
//            job.setCombinerClass(AvgReducer.class);

                //Setting Input and Output formats
                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(Text.class);
                job.setMapOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                //Setting Number of Reducers
                job.setNumReduceTasks(0);

                //Specify Input Path
                FileInputFormat.addInputPath(job, new Path(args[0]));

                //Specify Output Path
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                boolean complete = job.waitForCompletion(true);

                Configuration conf2 = new Configuration();
                Job job1 = Job.getInstance(conf2,"Chaining Count");
                if (complete) {

                    job1.setJarByClass(TopProductsByYear.class);


                    //Assigning the mapper, reducer, combiner classes
                    job1.setMapperClass(ReviewCountOnProductIdMapper.class);
                    job1.setReducerClass(ReviewCountOnProductIdReducer.class);
                    job1.setCombinerClass(ReviewCountOnProductIdReducer.class);

                    //Setting Input and Output formats
                    job1.setInputFormatClass(TextInputFormat.class);
                    job1.setOutputFormatClass(TextOutputFormat.class);
                    job1.setOutputKeyClass(CustomWritableKeyClass.class);
                    job1.setOutputValueClass(LongWritable.class);
                    job1.setMapOutputKeyClass(CustomWritableKeyClass.class);
                    job1.setOutputValueClass(LongWritable.class);
//                    job1.setPartitionerClass(CustomPartitioner.class);

                    //Setting Number of Reducers
                    job1.setNumReduceTasks(1);

                    //Specify Input Path
                    FileInputFormat.addInputPath(job1, new Path(args[1]));

                    //Specify Output Path
                    FileOutputFormat.setOutputPath(job1, new Path(args[2]));

                    boolean complete1 = job1.waitForCompletion(true);

                    Configuration conf3 = new Configuration();
                    Job job2 = Job.getInstance(conf3,"Chaining Binning");
                    if(complete1){

                        job2.setJarByClass(TopProductsByYear.class);


                        //Assigning the mapper, reducer, combiner classes
                        job2.setMapperClass(BinningByYear.class);
//                        job2.setReducerClass(Top5ByYearReducer.class);
//                        job2.setCombinerClass(Top5ByYearReducer.class);

                        //Setting Input and Output formats
                        job2.setInputFormatClass(TextInputFormat.class);
                        MultipleOutputs.addNamedOutput(job2,"bins",TextOutputFormat.class,Text.class,NullWritable.class);
                        MultipleOutputs.setCountersEnabled(job2,true);
//                        job2.setOutputKeyClass(CustomWritableCountClass.class);
//                        job2.setOutputValueClass(IntWritable.class);
                        job2.setMapOutputKeyClass(Text.class);
                        job2.setMapOutputValueClass(NullWritable.class);
//                        job2.setPartitionerClass(CustomPartitioner.class);

                        //Setting Number of Reducers
                        job2.setNumReduceTasks(0);

                        //Specify Input Path
                        FileInputFormat.addInputPath(job2, new Path(args[2]));

                        //Specify Output Path
                        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

                         System.exit(job2.waitForCompletion(true)?0:1);

                    }

                }


            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
