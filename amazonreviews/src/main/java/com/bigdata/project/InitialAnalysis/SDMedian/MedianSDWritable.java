package com.bigdata.project.InitialAnalysis.SDMedian;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MedianSDWritable implements Writable {

    private float sdRating;
    private float medianRating;


    public MedianSDWritable() {
    }

    public MedianSDWritable(float sdRating, float medianRating) {
        this.sdRating = sdRating;
        this.medianRating = medianRating;
    }

    public float getSdRating() {
        return sdRating;
    }

    public void setSdRating(float sdRating) {
        this.sdRating = sdRating;
    }

    public float getMedianRating() {
        return medianRating;
    }

    public void setMedianRating(float medianRating) {
        this.medianRating = medianRating;
    }

    @Override
    public String toString() {
        return sdRating +"\t" + medianRating ;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(sdRating);
        dataOutput.writeFloat(medianRating);
    }

    public void readFields(DataInput dataInput) throws IOException {
       sdRating= dataInput.readFloat();
        medianRating = dataInput.readFloat();
    }
}
