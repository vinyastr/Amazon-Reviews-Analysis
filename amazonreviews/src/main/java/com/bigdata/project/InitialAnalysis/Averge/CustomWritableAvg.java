package com.bigdata.project.InitialAnalysis.Averge;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritableAvg implements Writable {

    private float rating =0;
    private long count=0;


    public CustomWritableAvg() {
    }

    public CustomWritableAvg(long rating, long count) {
        this.rating = rating;
        this.count = count;
    }

    public float getRating() {
        return rating;
    }

    public void setRating(float rating) {
        this.rating = rating;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(rating);
        dataOutput.writeLong(count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        rating = dataInput.readFloat();
        count = dataInput.readLong();
    }

    @Override
    public String toString() {
        return String.valueOf(rating) ;
    }
}
