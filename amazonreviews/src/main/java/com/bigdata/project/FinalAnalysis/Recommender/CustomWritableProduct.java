package com.bigdata.project.FinalAnalysis.Recommender;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomWritableProduct implements Writable {

    private String productId;
    private String starRating;

    public CustomWritableProduct() {
    }

    public CustomWritableProduct(String productId, String starRating) {
        this.productId = productId;
        this.starRating = starRating;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getStarRating() {
        return starRating;
    }

    public void setStarRating(String starRating) {
        this.starRating = starRating;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(productId);
        dataOutput.writeUTF(starRating);

    }

    public void readFields(DataInput dataInput) throws IOException {
        productId=dataInput.readUTF();
        starRating=dataInput.readUTF();
    }

    @Override
    public String toString() {
        return productId + '\t' + starRating;
    }
}
