package com.bigdata.project.FinalAnalysis.Recommender;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.util.List;

public class RecommenderSys  {

    public static void main(String args[]){

        try {

            DataModel model = new FileDataModel(new File("/Users/vinyaskaushiktr/Downloads/RecommenderOut/part-r-00000"));
            UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
            UserNeighborhood neighborhood = new NearestNUserNeighborhood(5, similarity,model);

            Recommender recommender = new GenericUserBasedRecommender(model,neighborhood,similarity);

            List<RecommendedItem> recommendations = recommender.recommend(10020138,1);
                System.out.println(recommendations.size());
            for(RecommendedItem recommendation : recommendations){

                System.out.println(recommendation.getItemID()+" "+recommendation.getValue());
            }

        }catch (Exception ex){
            ex.printStackTrace();
        }

    }
}
