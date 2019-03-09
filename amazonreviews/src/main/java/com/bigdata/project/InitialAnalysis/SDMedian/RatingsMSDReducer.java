package com.bigdata.project.InitialAnalysis.SDMedian;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class RatingsMSDReducer extends Reducer<Text, SortedMapWritable,Text,MedianSDWritable> {

    private MedianSDWritable result = new MedianSDWritable();
    private TreeMap<Integer,Long> ratings= new TreeMap<Integer,Long>();

    @Override
    protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {

        float sum=0;
        long totalRatings=0;
        ratings.clear();
        result.setMedianRating(0);
        result.setSdRating(0);

        for(SortedMapWritable v:values){
          for(Object e : v.entrySet()){
            Entry<WritableComparable,Writable> entry =  (Entry<WritableComparable,Writable>)e;
            int rating = ((IntWritable)entry.getKey()).get();
            long count = ((LongWritable)entry.getValue()).get();

            totalRatings += count;
            sum += rating*count;

            Long storedCount = ratings.get(rating);
            if(storedCount==null){
                ratings.put(rating,count);
            }
            else {
                ratings.put(rating,storedCount+count);
            }
          }
          v.clear();
        }

        long medianIndex = totalRatings/2;
        long previousRating = 0;
        long rating = 0;
        int preKey=0;
        for(Entry<Integer, Long> entry :ratings.entrySet()){
            rating = previousRating+entry.getValue();

            if(previousRating <= medianIndex && medianIndex <rating){
                if(totalRatings %2 ==0 && previousRating == medianIndex ){
                    result.setMedianRating((float)(entry.getKey()+preKey)/2.0f);
                }
                else {
                    result.setMedianRating(entry.getKey());
                }
                break;
            }
            previousRating = rating;
            preKey = entry.getKey();
        }

        //calculate StandardDeviation
        float mean = sum/totalRatings;
        float sumOfSquares = 0.0f;

        for (Entry<Integer, Long> entry :ratings.entrySet()){
            sumOfSquares+=(entry.getKey()- mean)*(entry.getKey() -mean) * entry.getValue();
        }

        result.setSdRating((float)Math.sqrt(sumOfSquares/(totalRatings-1)));
        context.write(key,result);
    }
}
